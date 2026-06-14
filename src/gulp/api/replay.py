"""Helpers for request replay markers on mutable document side effects."""

UPDATE_REQUEST_IDS_FIELD = "gulp.update_req_ids"


def document_has_update_request(doc: dict, req_id: str | None) -> bool:
    """Return True if a document already records an update for ``req_id``."""
    if not req_id:
        return False

    markers = doc.get(UPDATE_REQUEST_IDS_FIELD) or []
    if not isinstance(markers, list):
        return False
    return req_id in markers


def mark_document_update_request(doc: dict, req_id: str | None) -> None:
    """Append ``req_id`` to the document update marker list if available."""
    if not req_id:
        return

    markers = doc.get(UPDATE_REQUEST_IDS_FIELD)
    if not isinstance(markers, list):
        markers = []
    if req_id not in markers:
        markers.append(req_id)
    doc[UPDATE_REQUEST_IDS_FIELD] = markers
