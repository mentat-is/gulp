import React, { ReactNode, useEffect, useMemo, useRef, useState } from "react";
import { createPortal } from "react-dom";
import { Application } from "@/context/Application.context";

const STYLE_MARKER = "data-gulp-opener-dialog-style";
const PRESENTATION_ATTRIBUTES = ["class", "data-theme", "dir", "lang", "style"];

interface DetachedSurface {
	container: HTMLDivElement;
	document: Document;
	window: Window;
}

export interface OpenerDetachedDialogProps {
	application: Application.Context.Props;
	children: ReactNode;
	features: string;
	name: string;
	title: string;
	onBlocked: () => void;
	onClosed: () => void;
}

/**
 * Copies the application styles into an about:blank popup document.
 * @param source Main application document.
 * @param target Popup document receiving the cloned style/link elements.
 */
function copyDocumentStyles(source: Document, target: Document): void {
	target.head.querySelectorAll(`[${STYLE_MARKER}]`).forEach((node) => node.remove());
	source.head
		.querySelectorAll<HTMLLinkElement | HTMLStyleElement>(
			'link[rel="stylesheet"], style',
		)
		.forEach((node) => {
			const clone = node.cloneNode(true) as HTMLLinkElement | HTMLStyleElement;
			clone.setAttribute(STYLE_MARKER, "");
			if (node.tagName === "LINK" && clone.tagName === "LINK") {
				(clone as HTMLLinkElement).href = (node as HTMLLinkElement).href;
			}
			target.head.appendChild(clone);
		});
}

/**
 * Mirrors root presentation attributes used by next-themes and global CSS.
 * @param source Main application document.
 * @param target Popup document.
 */
function syncDocumentPresentation(source: Document, target: Document): void {
	PRESENTATION_ATTRIBUTES.forEach((attribute) => {
		const value = source.documentElement.getAttribute(attribute);
		if (value === null) target.documentElement.removeAttribute(attribute);
		else target.documentElement.setAttribute(attribute, value);
	});
}

/**
 * Hosts a non-serializable React dialog in a popup while retaining the main
 * application realm. The scoped context changes only `currentDocument`, so
 * plugin API/state mutations keep their authoritative main ownership while
 * document-aware overlays and keyboard handlers target the popup.
 */
export function OpenerDetachedDialog({
	application,
	children,
	features,
	name,
	title,
	onBlocked,
	onClosed,
}: OpenerDetachedDialogProps) {
	const [surface, setSurface] = useState<DetachedSurface | null>(null);
	const onBlockedRef = useRef(onBlocked);
	const onClosedRef = useRef(onClosed);
	onBlockedRef.current = onBlocked;
	onClosedRef.current = onClosed;

	useEffect(() => {
		const popup = window.open("", name, features);
		if (!popup) {
			onBlockedRef.current();
			return;
		}

		const popupDocument = popup.document;
		popupDocument.title = title;
		copyDocumentStyles(document, popupDocument);
		syncDocumentPresentation(document, popupDocument);

		const container = popupDocument.createElement("div");
		container.id = "opener-detached-dialog-root";
		container.style.height = "100vh";
		container.style.width = "100%";
		popupDocument.body.replaceChildren(container);

		const presentationObserver = new MutationObserver(() =>
			syncDocumentPresentation(document, popupDocument),
		);
		presentationObserver.observe(document.documentElement, {
			attributes: true,
			attributeFilter: PRESENTATION_ATTRIBUTES,
		});

		let mounted = true;
		const handlePopupClose = () => {
			if (!mounted) return;
			mounted = false;
			setSurface(null);
			onClosedRef.current();
		};
		popup.addEventListener("beforeunload", handlePopupClose, { once: true });
		setSurface({ container, document: popupDocument, window: popup });
		popup.focus();

		return () => {
			mounted = false;
			presentationObserver.disconnect();
			popup.removeEventListener("beforeunload", handlePopupClose);
			if (!popup.closed) popup.close();
		};
	}, [features, name, title]);

	const popupApplication = useMemo<Application.Context.Props | null>(
		() =>
			surface
				? {
						...application,
						currentDocument: surface.document,
					}
				: null,
		[application, surface],
	);

	if (!surface || !popupApplication || surface.window.closed) return null;

	return createPortal(
		<Application.Context.Provider value={popupApplication}>
			{children}
		</Application.Context.Provider>,
		surface.container,
	);
}
