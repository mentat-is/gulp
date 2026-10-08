"""
Read-only observation of the http requests served by gULP, for extension plugins.

An extension plugin is loaded once the application has started, when FastAPI no
longer accepts `app.add_middleware()`, and a middleware could change or block
what it sees anyway. A request observer only looks: the core hands it a frozen
copy of the request when it arrives and of its outcome when it is answered, and
never the channels the request and the response travel on.

Observers are registered with `GulpServer.add_request_observer()`; the
dispatcher below, installed by the core before the application starts, calls
them.
"""

import time
from dataclasses import dataclass
from types import MappingProxyType
from typing import Mapping

from starlette.types import ASGIApp, Message, Receive, Scope, Send

from gulp.glug import Glug

# the most an observer can ask of a request or response body, whatever it asks for
OBSERVED_BODY_MAX_BYTES: int = 64 * 1024


@dataclass(frozen=True, slots=True)
class GulpObservedRequest:
    """an http request as an observer sees it: a copy, nothing in it reaches the handler."""

    method: str
    path: str
    query_string: str
    # header names are lowercase; a header sent more than once has its values joined by ", "
    headers: Mapping[str, str]
    # the address of the client, None when the server does not know it
    client_host: str | None


@dataclass(frozen=True, slots=True)
class GulpObservedResponse:
    """how a request ended, as an observer sees it."""

    # the path of the matched route as it was declared, None when no route matched
    route: str | None
    # None when the application raised, or the client left, before any answer was sent
    status_code: int | None
    duration_msec: float
    # the first bytes of the bodies, as many as the observer asked for (see GulpObservation)
    request_body: bytes
    response_body: bytes


@dataclass(frozen=True, slots=True)
class GulpObservation:
    """what an observer wants of a request it is interested in."""

    # how many bytes of each body to receive in on_request_end(), at most
    # OBSERVED_BODY_MAX_BYTES: 0 (the default) means none, and costs nothing
    request_body_bytes: int = 0
    response_body_bytes: int = 0


class GulpRequestObserver:
    """
    base class of a request observer: override one or both methods.

    both are plain functions on purpose, called on the path of the request: they must
    return quickly and do no I/O. an observer that has work to do hands it to a task of
    its own. an exception raised here is logged and does not affect the request.
    """

    def on_request_start(self, request: GulpObservedRequest) -> GulpObservation | None:
        """
        called when a request arrives, before any core middleware and before
        authentication, in the context of the request: a contextvar set here is
        seen by the handler.

        Args:
            request (GulpObservedRequest): the request

        Returns:
            GulpObservation | None: None when the observer is not interested in this
                request (nothing else is done for it, on_request_end() is not called),
                otherwise what it wants of it
        """
        return None

    def on_request_end(
        self, request: GulpObservedRequest, response: GulpObservedResponse
    ) -> None:
        """
        called once the request has been answered (or has failed), only for the
        requests on_request_start() returned a GulpObservation for.

        Args:
            request (GulpObservedRequest): the same object given to on_request_start()
            response (GulpObservedResponse): how the request ended
        """


class RequestObservers:
    """the request observers registered by extension plugins, in registration order."""

    def __init__(self) -> None:
        # replaced, never changed in place: a request being served keeps iterating
        # the tuple it started with
        self._observers: tuple[GulpRequestObserver, ...] = ()

    def observers(self) -> tuple[GulpRequestObserver, ...]:
        """
        Returns:
            tuple[GulpRequestObserver, ...]: the registered observers
        """
        return self._observers

    def add(self, observer: GulpRequestObserver) -> bool:
        """
        Args:
            observer (GulpRequestObserver): the observer to register

        Returns:
            bool: False if it was already registered (nothing changes), True otherwise
        """
        if observer in self._observers:
            return False
        self._observers = (*self._observers, observer)
        return True

    def remove(self, observer: GulpRequestObserver) -> bool:
        """
        Args:
            observer (GulpRequestObserver): the observer to remove

        Returns:
            bool: False if it was not registered (nothing changes), True otherwise
        """
        if observer not in self._observers:
            return False
        self._observers = tuple(o for o in self._observers if o is not observer)
        return True

    def clear(self) -> None:
        """removes every registered observer"""
        self._observers = ()


def _body_limit(asked: int) -> int:
    """the number of body bytes an observer gets, given what it asked for"""
    return max(0, min(int(asked), OBSERVED_BODY_MAX_BYTES))


def _observed_request(scope: Scope) -> GulpObservedRequest:
    """builds the copy of the request handed to the observers"""
    headers: dict[str, str] = {}
    for raw_name, raw_value in scope.get("headers") or ():
        name: str = raw_name.decode("latin-1").lower()
        value: str = raw_value.decode("latin-1")
        headers[name] = f"{headers[name]}, {value}" if name in headers else value
    client = scope.get("client")
    return GulpObservedRequest(
        method=scope.get("method", ""),
        path=scope.get("path", ""),
        query_string=(scope.get("query_string") or b"").decode("latin-1"),
        headers=MappingProxyType(headers),
        client_host=client[0] if client else None,
    )


class RequestObserverDispatcher:
    """
    ASGI middleware installed by the core, which tells the registered observers
    about each http request without letting them touch it.
    """

    def __init__(self, app: ASGIApp, registry: RequestObservers):
        self.app = app
        self._registry = registry

    async def __call__(self, scope: Scope, receive: Receive, send: Send) -> None:
        observers = self._registry.observers()
        if scope["type"] != "http" or not observers:
            await self.app(scope, receive, send)
            return

        request: GulpObservedRequest = _observed_request(scope)

        # only the observers interested in this request, with what each one asked for
        interested: list[tuple[GulpRequestObserver, int, int]] = []
        for observer in observers:
            try:
                observation = observer.on_request_start(request)
                if observation is not None:
                    interested.append(
                        (
                            observer,
                            _body_limit(observation.request_body_bytes),
                            _body_limit(observation.response_body_bytes),
                        )
                    )
            except Exception:
                Glug.get_instance().exception(
                    f"request observer {observer!r} failed in on_request_start"
                )
        if not interested:
            await self.app(scope, receive, send)
            return

        request_limit: int = max(limit for _, limit, _ in interested)
        response_limit: int = max(limit for _, _, limit in interested)
        request_body = bytearray()
        response_body = bytearray()
        status_code: int | None = None

        async def receive_copying() -> Message:
            message: Message = await receive()
            if message["type"] == "http.request" and len(request_body) < request_limit:
                request_body.extend(
                    message.get("body", b"")[: request_limit - len(request_body)]
                )
            return message

        async def send_copying(message: Message) -> None:
            nonlocal status_code
            if message["type"] == "http.response.start":
                status_code = message["status"]
            elif (
                message["type"] == "http.response.body"
                and len(response_body) < response_limit
            ):
                response_body.extend(
                    message.get("body", b"")[: response_limit - len(response_body)]
                )
            await send(message)

        started: float = time.monotonic()
        try:
            # the bodies are only copied as they go by: nothing is held back or changed
            await self.app(
                scope, receive_copying if request_limit else receive, send_copying
            )
        finally:
            duration_msec: float = (time.monotonic() - started) * 1000
            route: str | None = getattr(scope.get("route"), "path", None)
            for observer, observer_request_limit, observer_response_limit in interested:
                try:
                    observer.on_request_end(
                        request,
                        GulpObservedResponse(
                            route=route,
                            status_code=status_code,
                            duration_msec=duration_msec,
                            request_body=bytes(request_body[:observer_request_limit]),
                            response_body=bytes(
                                response_body[:observer_response_limit]
                            ),
                        ),
                    )
                except Exception:
                    Glug.get_instance().exception(
                        f"request observer {observer!r} failed in on_request_end"
                    )
