import { Application } from "@/context/Application.context";
import { Dialog } from "@/ui/Dialog";
import { Banner as UIBanner } from "@/ui/Banner";
import {
	type CSSProperties,
	type MouseEvent as ReactMouseEvent,
	useEffect,
	useMemo,
	useState,
	useCallback,
	useRef,
	useSyncExternalStore,
} from "react";
import s from "./styles/DisplayEventDialog.module.css";
import {
	copy,
	download,
	generateUUID,
	isPlainObject,
	sortObjectKeysRecursively as sortTreeValueRecursively,
} from "@/ui/utils";
import { Stack } from "@/ui/Stack";
import { Button } from "@/ui/Button";
import { Skeleton } from "@/ui/Skeleton";
import { Navigation } from "./components/navigation";
import { Enrichment } from "@/banners/Enrichment.banner";
import {
	LinkFunctionality,
	NoteFunctionality,
} from "@/banners/Collab.functionality";
import { Collab } from "@/components/CollabList";
import {
	ContextMenu,
	ContextMenuContent,
	ContextMenuItem,
	ContextMenuTrigger,
} from "@/ui/ContextMenu";
import { FilterFileBanner } from "@/banners/FilterFile.banner";
import { SendData } from "@/banners/SendData.banner";
import { toast } from "sonner";
import {
	JsonView,
	allExpanded,
	darkStyles,
	defaultStyles,
} from "react-json-view-lite";
import { StyleProps } from "react-json-view-lite/dist/DataRenderer";
import { useTheme } from "next-themes";
import { Tabs, TabsContent, TabsList, TabsTrigger } from "@/ui/Tabs";
import { Popover } from "@/ui/Popover";
import { Table } from "@/components/Table";
import { Markdown } from "@/ui/Markdown";
import { Icon } from "@/ui/Icon";
import { cn } from "@/ui/utils";
import { Doc } from "@/entities/Doc";
import { Source } from "@/entities/Source";
import { Filter } from "@/entities/Filter";
import { Note } from "@/entities/Note";
import { Link } from "@/entities/Link";
import type { App } from "@/entities/App";
import type { Operation } from "@/entities/Operation";
import { Extension } from "@/context/Extension.context";
import { Locale } from "@/locales";
import { DisplayGroupDialog } from "./Group.dialog";
import { DataStore } from "@/store/DataStore";
import { EventJsonTransforms } from "@/lib/EventJsonTransforms";
import { resolveEventRenderColor } from "@/utils";
import { useEventDetail } from "./useEventDetail";
import { useEventDetailViewPreference } from "./useEventDetailView";
import type { WindowBridge } from "@/lib/WindowBridge";
import { getEventIndicatorPresentation } from "@/event-store/EventIndicatorValue";

// --- UTILITIES ---

interface EventLinkTableRow {
	id: Link.Id;
	_id: Link.Id;
	name: string;
	description: string;
	color: string;
	icon: Icon.Name;
}

type EventActionSource = "manual" | "auto";
type EventActionView = "tree" | "raw" | "table";

interface ResolvedEventValueTarget {
	view?: EventActionView;
	fieldPath?: string;
	field?: string;
	value: string;
	displayText: string;
	selectionRange?: Range;
}

interface EventActionTarget extends ResolvedEventValueTarget {
	source: EventActionSource;
}

/**
 * Resolves every document related to a link, including the root document and
 * connected destination documents, while preserving stored link order.
 * @param link Link whose related events should be displayed.
 * @returns Related documents that are currently available in the document index.
 */
async function getRelatedLinkEvents(link: Link.Type): Promise<Doc.Type[]> {
	const relatedDocumentIds: Doc.Id[] = [];
	const seenDocIds = new Set<Doc.Id>();

	for (const docId of [link.doc_id_from, ...link.doc_ids]) {
		if (seenDocIds.has(docId)) continue;
		seenDocIds.add(docId);
		relatedDocumentIds.push(docId);
	}

	return DataStore.Events.getByIds(relatedDocumentIds);
}

/**
 * Resolves whether an event is flagged within its source operation.
 * @param app Current application state used to resolve the event operation.
 * @param event Event whose flag state should be checked.
 * @returns True when the event is flagged for its operation; otherwise false.
 */
function getEventFlagState(app: App.Type, event: Doc.Type): boolean {
	const operationId = Doc.Entity.operationId(app, event);
	return operationId ? Doc.Entity.flag.isFlagged(event._id, operationId) : false;
}

const INITIAL_COLLAB_PANEL_HEIGHT = 240;
const MIN_COLLAB_PANEL_HEIGHT = 140;
const RESERVED_EVENT_VIEWER_HEIGHT = 260;

/**
 * Removes array indices from a dot-separated path.
 * Example: `gulp.unmapped.Data.0.key` -> `gulp.unmapped.Data.key`
 */
const stripArrayIndices = (path: string): string => {
	return path
		.split(".")
		.filter((part) => !/^\d+$/.test(part))
		.join(".");
};

/**
 * Converts an event value into the string used by actions.
 * @param value Event value resolved from the JSON document.
 * @returns String value suitable for note/filter/enrichment payloads.
 */
const stringifyEventActionValue = (value: unknown): string => {
	if (value === undefined) return "";
	if (value === null) return "null";
	if (typeof value === "string") return value;
	if (typeof value === "bigint") return value.toString();
	return JSON.stringify(value) ?? String(value);
};

/**
 * Converts an event value into a JSON-like value preview for auto-selection text.
 * @param value Event value resolved from the JSON document.
 * @returns Display-safe value text.
 */
const stringifyEventDisplayValue = (value: unknown): string => {
	if (typeof value === "string") return `"${value}"`;
	if (typeof value === "bigint") return `"${value.toString()}"`;
	return JSON.stringify(value) ?? "undefined";
};

/**
 * Builds the display text used when the dialog auto-selects a full field/value pair.
 * @param field Field shown to the user.
 * @param value Event value resolved from the JSON document.
 * @returns JSON-like field/value text.
 */
const formatEventDisplayText = (field: string, value: unknown): string => {
	return `"${field}": ${stringifyEventDisplayValue(value)}`;
};

/**
 * Converts a JSON literal from the raw view into the value used by actions.
 * @param rawValue Raw value text detected in the JSON view.
 * @returns Parsed string value when possible, or the cleaned raw literal.
 */
const parseRawJsonActionValue = (rawValue: string): string => {
	try {
		return stringifyEventActionValue(JSON.parse(rawValue));
	} catch {
		return rawValue
			.trim()
			.replace(/^"+|"+$/g, "")
			.replace(/[,"]+$/g, "");
	}
};

/**
 * Clamps the collaboration panel height while preserving room for the event viewer.
 *
 * @param value Requested panel height in pixels.
 * @param viewportHeight Current owner window height in pixels.
 * @returns Bounded panel height in pixels.
 */
const clampCollabPanelHeight = (
	value: number,
	viewportHeight: number,
): number => {
	const maximumPanelHeight = Math.max(
		MIN_COLLAB_PANEL_HEIGHT,
		viewportHeight - RESERVED_EVENT_VIEWER_HEIGHT,
	);

	return Math.max(MIN_COLLAB_PANEL_HEIGHT, Math.min(value, maximumPanelHeight));
};

/**
 * Removes 'event.original' from an object and re-inserts it at the end.
 * @param obj The source object
 * @returns A new object with 'event.original' at the end
 */
const prepareEventJson = (
	obj: object,
): Record<string, unknown> => {
	const source = obj as Record<string, unknown>;
	const entries = Object.entries(source).filter(([k]) => k !== "event.original");
	return {
		...Object.fromEntries(entries),
		"event.original": source["event.original"],
	};
};

/**
 * Flattens a nested object into an array of key-value pairs suitable for table display.
 * @param value The value to flatten
 * @param parentKey The current accumulated key path
 */
const flattenTableEntries = (
	value: unknown,
	parentKey = "",
): Array<{ key: string; value: string }> => {
	if (Array.isArray(value)) {
		return value.flatMap((entry, index) =>
			flattenTableEntries(
				entry,
				parentKey ? `${parentKey}.${index}` : String(index),
			),
		);
	}

	if (value && typeof value === "object") {
		return Object.entries(value as Record<string, unknown>).flatMap(
			([key, entry]) =>
				flattenTableEntries(entry, parentKey ? `${parentKey}.${key}` : key),
		);
	}

	return [
		{
			key: parentKey,
			value:
				value == null
					? String(value)
					: typeof value === "bigint"
						? value.toString()
						: String(value),
		},
	];
};

/**
 * Reconstructs the full dot-notated key path from a Tree label element.
 * @param labelEl The label element to start from
 * @param labelClass CSS class for labels
 * @param clickableLabelClass CSS class for clickable labels
 * @param nodeClass CSS class for nodes
 * @param basicClass CSS class for basic containers
 */
const getTreeKeyPath = (
	labelEl: Element,
	labelClass: string,
	clickableLabelClass: string,
	nodeClass: string,
	basicClass: string,
): string => {
	const clean = (el: Element) => {
		let text = (el.textContent || "").trim();
		// Remove all quotes
		text = text.replace(/"/g, "");
		// Remove trailing colon, comma, and whitespace
		text = text.replace(/[:, \s]+$/, "");
		return text;
	};
	const parts: string[] = [clean(labelEl)];

	let nodeEl: Element | null = labelEl.closest(`.${nodeClass}`);
	while (nodeEl) {
		const parentContainer = nodeEl.parentElement;
		if (!parentContainer?.classList.contains(basicClass)) break;
		const parentNode = parentContainer.parentElement;
		if (!parentNode?.classList.contains(nodeClass)) break;

		const parentLabel = Array.from(parentNode.children).find(
			(c) =>
				c.classList.contains(labelClass) ||
				c.classList.contains(clickableLabelClass),
		);
		if (!parentLabel) break;
		parts.unshift(clean(parentLabel));
		nodeEl = parentNode;
	}
	return parts.join(".");
};

/**
 * Resolves a dot-separated path within a JSON object.
 * Handles both flat maps and nested structures, including array indices.
 * @param json Source data object
 * @param path Dot-separated path to resolve
 */
const getValueByPath = (
	json: Record<string, unknown> | null,
	path: string,
): unknown => {
	if (!json) return undefined;
	if (Object.prototype.hasOwnProperty.call(json, path)) {
		return json[path];
	}

	const parts = path.split(".");
	let current: unknown = json;

	// Try to traverse using dots
	for (const part of parts) {
		if (current && typeof current === "object") {
			// Try direct property access first
			if (Object.prototype.hasOwnProperty.call(current, part)) {
				current = (current as Record<string, unknown>)[part];
			}
			// For arrays, also try numeric index
			else if (Array.isArray(current) && /^\d+$/.test(part)) {
				const index = parseInt(part, 10);
				current = (current as unknown[])[index];
			} else {
				// If we can't find it via dots, try checking if there's a parent key with dots
				// For example, if path is "gulp.unmapped.Guid" and parts[0:2] joined as "gulp.unmapped" exists
				for (let i = parts.length - 1; i > 0; i--) {
					const parentPath = parts.slice(0, i).join(".");
					const childPart = parts[i];

					if (Object.prototype.hasOwnProperty.call(json, parentPath)) {
						const parentObj = json[parentPath];
						if (
							parentObj &&
							typeof parentObj === "object" &&
							Object.prototype.hasOwnProperty.call(parentObj, childPart)
						) {
							return (parentObj as Record<string, unknown>)[childPart];
						}
					}
				}
				return undefined;
			}
		} else {
			return undefined;
		}
	}

	return current;
};

const isTreeLeafValue = (
	json: Record<string, unknown> | null,
	path: string | null,
): boolean => {
	if (!json || !path) return false;
	const value = getValueByPath(json, path);
	return value !== undefined && !isPlainObject(value);
};

/**
 * Checks if a coordinate point is within the current browser selection.
 * @param x Client X
 * @param y Client Y
 * @returns boolean
 */
const isPointInSelection = (doc: Document, x: number, y: number): boolean => {
	const selection = doc.defaultView?.getSelection();
	if (!selection || selection.rangeCount === 0 || selection.isCollapsed)
		return false;
	const range = selection.getRangeAt(0);
	const rects = range.getClientRects();
	for (let i = 0; i < rects.length; i++) {
		const rect = rects[i];
		if (x >= rect.left && x <= rect.right && y >= rect.top && y <= rect.bottom)
			return true;
	}
	return false;
};

/**
 * Resolves the key-value pair under the cursor in the Table view.
 * @param doc Current document used to create a selectable row range.
 * @param element The clicked element.
 * @returns Resolved action target data for the clicked row.
 */
const resolveTableEventValueTarget = (
	doc: Document,
	element: HTMLElement,
): ResolvedEventValueTarget | null => {
	const tr = element.closest("tr");
	if (tr) {
		const cells = Array.from(tr.querySelectorAll("td"));
		if (cells.length >= 2) {
			const range = doc.createRange();
			range.selectNodeContents(tr);
			const rawKey = cells[0].innerText.trim();
			const cleanedKey = stripArrayIndices(rawKey);
			const value = cells[1].innerText.trim();
			return {
				view: "table",
				fieldPath: rawKey,
				field: cleanedKey,
				value,
				displayText: formatEventDisplayText(cleanedKey, value),
				selectionRange: range,
			};
		}
	}
	return null;
};

/**
 * Detects the full key-value path under the cursor in the Tree view.
 * @param element The clicked element
 * @param json The source JSON object
 */
const resolveTreeContextPath = (
	element: HTMLElement,
	json: Record<string, unknown> | null,
): string | null => {
	if (!json) return null;

	const clickedNode = element.closest(`.${s.node}`) as HTMLElement | null;
	if (!clickedNode) return null;

	const arrayContainer = clickedNode.parentElement;
	if (!arrayContainer || !arrayContainer.classList.contains(s.basic)) {
		return null;
	}

	const parentFieldNode = arrayContainer.parentElement?.closest(`.${s.node}`);
	if (!parentFieldNode) return null;

	const parentLabel = Array.from(parentFieldNode.children).find(
		(child) =>
			child.classList.contains(s.label) ||
			child.classList.contains(s.clickableLabel),
	);
	if (!parentLabel) return null;

	const parentPath = getTreeKeyPath(
		parentLabel,
		s.label,
		s.clickableLabel,
		s.node,
		s.basic,
	);
	if (!parentPath) return null;

	const parentValue = getValueByPath(json, parentPath);
	if (!Array.isArray(parentValue)) return null;

	const index = Array.from(arrayContainer.children).indexOf(clickedNode);
	return index >= 0 ? `${parentPath}.${index}` : null;
};

/**
 * Builds a resolved Tree-view action target for a path and its DOM node.
 * @param path Full JSON path resolved from the tree.
 * @param value Value resolved from the event JSON.
 * @param selectionElement Element to select when auto-selecting the full value.
 * @param doc Current document used to create a range.
 * @returns Resolved Tree target.
 */
const createTreeEventValueTarget = (
	path: string,
	value: unknown,
	selectionElement: Element | null,
	doc: Document,
): ResolvedEventValueTarget => {
	const cleanedPath = stripArrayIndices(path);
	const selectionRange = selectionElement ? doc.createRange() : undefined;
	if (selectionRange && selectionElement) {
		selectionRange.selectNodeContents(selectionElement);
	}

	return {
		view: "tree",
		fieldPath: path,
		field: cleanedPath,
		value: stringifyEventActionValue(value),
		displayText: formatEventDisplayText(cleanedPath, value),
		selectionRange,
	};
};

/**
 * Resolves the key-value pair under the cursor in the Tree view.
 * @param doc Current document used to create selection ranges.
 * @param element The clicked element.
 * @param json Source event JSON object.
 * @returns Resolved action target data for the clicked tree value.
 */
const resolveTreeEventValueTarget = (
	doc: Document,
	element: HTMLElement,
	json: Record<string, unknown> | null,
): ResolvedEventValueTarget | null => {
	let treeLabel: Element | null =
		element.classList.contains(s.label) ||
		element.classList.contains(s.clickableLabel)
			? element
			: (element.closest(`.${s.label}`) ??
				element.closest(`.${s.clickableLabel}`));

	if (!treeLabel) {
		const node = element.closest(`.${s.node}`);
		if (node) {
			// Find the DIRECT child label, not the first descendant label
			// This is important for arrays to avoid getting the first item's label
			treeLabel =
				Array.from(node.children).find(
					(child) =>
						child.classList.contains(s.label) ||
						child.classList.contains(s.clickableLabel),
				) ?? null;
		}
	}

	// If still no label found, try to find any node ancestor that has a label
	if (!treeLabel) {
		let current = element.closest(`.${s.node}`);
		while (current) {
			const label = Array.from(current.children).find(
				(child) =>
					child.classList.contains(s.label) ||
					child.classList.contains(s.clickableLabel),
			);
			if (label) {
				treeLabel = label;
				break;
			}
			// Move to next ancestor node
			current = current.parentElement?.closest(`.${s.node}`) ?? null;
		}
	}

	// Last resort: if we're in a tree node but can't find a label, try to build path from node structure
	if (!treeLabel) {
		const node = element.closest(`.${s.node}`);
		if (node) {
			// Try to get any text that might be a key/index
			const textContent = node.textContent?.trim() || "";
			if (textContent && textContent.length > 0) {
				// This is a fallback - just try to determine if this could be a leaf value
				// by checking the element's text content
				const nodeParent = node.parentElement?.closest(`.${s.node}`);
				if (nodeParent) {
					const parentLabel = Array.from(nodeParent.children).find(
						(c) =>
							c.classList.contains(s.label) ||
							c.classList.contains(s.clickableLabel),
					);
					if (parentLabel) {
						// We have a parent label, try to build path from there
						const parentPath = getTreeKeyPath(
							parentLabel,
							s.label,
							s.clickableLabel,
							s.node,
							s.basic,
						);
						// For now, just use the parent value as we can't determine the exact child key
						if (parentPath && isTreeLeafValue(json, parentPath)) {
							const value = getValueByPath(json, parentPath);
							if (Array.isArray(value)) {
								// This is an array - we're clicking on an element within it
								// Try to find the index from the node's position
								const parent = node.parentElement;
								if (parent) {
									const siblingNodes = Array.from(
										parent.querySelectorAll(`.${s.node}`),
									);
									const index = siblingNodes.indexOf(node);
									if (index >= 0) {
										const childPath = `${parentPath}.${index}`;
										if (isTreeLeafValue(json, childPath)) {
											const childValue = getValueByPath(json, childPath);
											return createTreeEventValueTarget(
												childPath,
												childValue,
												node,
												doc,
											);
										}
									}
								}
							}
						}
					}
				}
			}
		}
	}

	if (treeLabel && json) {
		const resolvedPath = resolveTreeContextPath(element, json);
		const path =
			resolvedPath && isTreeLeafValue(json, resolvedPath)
				? resolvedPath
				: getTreeKeyPath(treeLabel, s.label, s.clickableLabel, s.node, s.basic);
		if (path && isTreeLeafValue(json, path)) {
			const value = getValueByPath(json, path);

			// Check if we're clicking on a child element of an array/object
			const labelNode = treeLabel.closest(`.${s.node}`) as HTMLElement;
			const clickedNode = element.closest(`.${s.node}`) as HTMLElement;

			// If clicked node is different from label node and value is an array/object,
			// try to find the actual child element
			if (
				labelNode &&
				clickedNode &&
				labelNode !== clickedNode &&
				(Array.isArray(value) || (typeof value === "object" && value !== null))
			) {
				// Find which child node we're in
				const parent = clickedNode.parentElement;
				if (parent) {
					const siblingNodes = Array.from(
						parent.querySelectorAll(`.${s.node}`),
					);
					const index = siblingNodes.indexOf(clickedNode);
					if (index >= 0 && Array.isArray(value)) {
						// This is an array, use the index to find the child value
						const childPath = `${path}.${index}`;
						const childValue = getValueByPath(json, childPath);
						if (childValue !== undefined) {
							return createTreeEventValueTarget(
								childPath,
								childValue,
								clickedNode,
								doc,
							);
						}
					}
				}
			}

			// Normal case: return the found path
			const nodeContainer = treeLabel.closest(`.${s.node}`) as HTMLElement;
			return createTreeEventValueTarget(path, value, nodeContainer, doc);
		}
	}
	return null;
};

/**
 * Creates a DOM range over text-node offsets inside a rendered code block.
 * @param doc Current document used to create the range and tree walker.
 * @param root Root node whose text content contains the requested offsets.
 * @param startIndex Inclusive character offset in the root text content.
 * @param endIndex Exclusive character offset in the root text content.
 * @returns A DOM range for the requested text span, or null when offsets cannot be resolved.
 */
const createTextNodeRange = (
	doc: Document,
	root: Node,
	startIndex: number,
	endIndex: number,
): Range | null => {
	const walker = doc.createTreeWalker(root, NodeFilter.SHOW_TEXT);
	let current = 0;
	let startNode: Text | null = null;
	let endNode: Text | null = null;
	let startOffset = 0;
	let endOffset = 0;

	while (walker.nextNode()) {
		const node = walker.currentNode as Text;
		const length = node.textContent?.length ?? 0;

		if (!startNode && current + length >= startIndex) {
			startNode = node;
			startOffset = startIndex - current;
		}

		if (!endNode && current + length >= endIndex) {
			endNode = node;
			endOffset = endIndex - current;
			break;
		}

		current += length;
	}

	if (!startNode || !endNode) return null;

	const range = doc.createRange();
	range.setStart(startNode, startOffset);
	range.setEnd(endNode, endOffset);
	return range;
};

/**
 * Finds the end offset of a JSON value starting at or after a given index.
 * @param text JSON source text.
 * @param startIndex Offset where value scanning should begin.
 * @returns Exclusive end offset for the JSON value.
 */
const findJsonValueEnd = (text: string, startIndex: number): number => {
	let i = startIndex;

	while (i < text.length && /\s/.test(text[i])) {
		i += 1;
	}

	if (i >= text.length) return i;

	const first = text[i];

	if (first === '"') {
		let escaped = false;
		i += 1;
		while (i < text.length) {
			const ch = text[i];
			if (escaped) {
				escaped = false;
			} else if (ch === "\\") {
				escaped = true;
			} else if (ch === '"') {
				return i + 1;
			}
			i += 1;
		}
		return text.length;
	}

	if (first === "{" || first === "[") {
		const stack: string[] = [first];
		let inString = false;
		let escaped = false;

		i += 1;
		while (i < text.length) {
			const ch = text[i];
			if (inString) {
				if (escaped) escaped = false;
				else if (ch === "\\") escaped = true;
				else if (ch === '"') inString = false;
				i += 1;
				continue;
			}

			if (ch === '"') {
				inString = true;
				i += 1;
				continue;
			}

			if (ch === "{" || ch === "[") {
				stack.push(ch);
			} else if (ch === "}" || ch === "]") {
				const open = stack.pop();
				if (!open) break;
				if ((open === "{" && ch === "}") || (open === "[" && ch === "]")) {
					if (stack.length === 0) return i + 1;
				} else {
					break;
				}
			}
			i += 1;
		}

		return text.length;
	}

	while (i < text.length) {
		const ch = text[i];
		if (/\s|,|\}|\]/.test(ch)) break;
		i += 1;
	}

	return i;
};

/**
 * Resolves the key-value pair under the cursor in the Raw JSON view.
 * @param doc Current document used to create value ranges.
 * @param element The clicked element.
 * @param x Client X coordinate.
 * @param y Client Y coordinate.
 * @returns Resolved action target data for the clicked raw JSON value.
 */
const resolveRawEventValueTarget = (
	doc: Document,
	element: HTMLElement,
	x: number,
	y: number,
): ResolvedEventValueTarget | null => {
	const highlighter = element.closest(`.${s.highlighter}`) as HTMLElement;
	const codeEl = highlighter?.querySelector("code, pre") as HTMLElement | null;

	let range: Range | null = null;
	if (doc.caretRangeFromPoint) {
		range = doc.caretRangeFromPoint(x, y);
	} else if (doc.caretPositionFromPoint) {
		const pos = doc.caretPositionFromPoint(x, y);
		if (pos) {
			range = doc.createRange();
			range.setStart(pos.offsetNode, pos.offset);
			range.setEnd(pos.offsetNode, pos.offset);
		}
	}

	if (range && codeEl) {
		const fullText = codeEl.textContent ?? codeEl.innerText ?? "";
		const strippedText = fullText
			.replace(/^```json\s*/g, "")
			.replace(/\s*```$/g, "");
		const jsonStart = fullText.indexOf(strippedText);
		if (jsonStart < 0) return null;

		const preRange = doc.createRange();
		preRange.selectNodeContents(codeEl);
		preRange.setEnd(range.startContainer, range.startOffset);
		const offset = preRange.toString().length;
		const clickIndexInJson = Math.max(0, offset - jsonStart);
		const lineStartIndex =
			strippedText.lastIndexOf("\n", clickIndexInJson - 1) + 1;
		const lineEndIndex = strippedText.indexOf("\n", lineStartIndex);
		const lineText = strippedText.slice(
			lineStartIndex,
			lineEndIndex < 0 ? undefined : lineEndIndex,
		);
		const targetLine = lineText;

		// Try to match key:value on the target line
		let match = targetLine.match(/^(\s*)"([^"]+)":/);
		let currentPath = match?.[2];
		let currentIndent = match?.[1].length;

		// If no key:value match, this might be an array/object value on its own line
		// Walk up to find the key
		if (!currentPath) {
			const lines = strippedText.split("\n");
			const targetLineIndex = lines.indexOf(targetLine);
			for (let i = targetLineIndex - 1; i >= 0; i -= 1) {
				const line = lines[i] ?? "";
				const m = line.match(/^(\s*)"([^"]+)":\s*[\{\[]/);
				if (m) {
					currentPath = m[2];
					currentIndent = m[1].length;
					break;
				}
			}
			if (!currentPath) return null;
		}

		// Build the full path by walking up to find parent keys
		const lines = strippedText.split("\n");
		for (let i = lines.indexOf(targetLine) - 1; i >= 0; i -= 1) {
			const line = lines[i] ?? "";
			const m = line.match(/^(\s*)"([^"]+)":\s*[\{\[]/);
			if (m) {
				const indent = m[1].length;
				if (currentIndent !== undefined && indent < currentIndent) {
					currentPath = `${m[2]}.${currentPath}`;
					currentIndent = indent;
				}
			}
			if (currentIndent === 0) break;
		}

		// Find the value boundaries
		const colonIndex = targetLine.indexOf(":");
		let valueStart: number;
		let valueEnd: number;

		if (colonIndex >= 0) {
			// Standard key:value line
			const firstNonWs = targetLine.slice(colonIndex + 1).search(/\S/);
			valueStart =
				lineStartIndex + colonIndex + 1 + (firstNonWs >= 0 ? firstNonWs : 0);
			valueEnd = findJsonValueEnd(strippedText, valueStart);
		} else {
			// Array/object value line - find the start of the value on this line
			const firstNonWs = targetLine.search(/\S/);
			if (firstNonWs < 0) return null;
			valueStart = lineStartIndex + firstNonWs;
			valueEnd = findJsonValueEnd(strippedText, valueStart);
		}

		const selectedText = strippedText.slice(valueStart, valueEnd).trim();
		const valueRange = createTextNodeRange(
			doc,
			codeEl,
			jsonStart + valueStart,
			jsonStart + valueEnd,
		);
		const cleanedPath = stripArrayIndices(currentPath);

		return {
			view: "raw",
			fieldPath: currentPath,
			field: cleanedPath,
			value: parseRawJsonActionValue(selectedText),
			displayText: `"${cleanedPath}": ${selectedText}`,
			selectionRange: valueRange ?? undefined,
		};
	}

	// Fallback for Raw View
	let current: HTMLElement | null = element;
	while (
		current &&
		current !== doc.body &&
		!current.classList.contains(s.highlighter)
	) {
		const text = (current.innerText || current.textContent || "").trim();
		if (text.includes(":")) {
			// Try to parse as JSON to detect if it's a complete key-value pair
			try {
				const match = text.match(/^"([^"]+)":\s*(.+)$/s);
				if (match) {
					const key = match[1];
					const valueStr = match[2];
					// Try parsing the value to see if it's valid JSON
					const parsedValue = JSON.parse(valueStr);
					const cleanedKey = stripArrayIndices(key);
					const selectionRange = doc.createRange();
					selectionRange.selectNodeContents(current);
					return {
						view: "raw",
						fieldPath: key,
						field: cleanedKey,
						value: stringifyEventActionValue(parsedValue),
						displayText: `"${cleanedKey}": ${stringifyEventDisplayValue(parsedValue)}`,
						selectionRange,
					};
				}
			} catch (e) {
				// Not a complete value or doesn't match pattern, try parent
			}

			// If doesn't include newlines, it's a simple single-line value, use it
			if (!text.includes("\n")) {
				const selectionRange = doc.createRange();
				selectionRange.selectNodeContents(current);
				return {
					view: "raw",
					value: text,
					displayText: text,
					selectionRange,
				};
			}
		}
		current = current.parentElement;
	}
	return null;
};

/**
 * Resolves the event value under a pointer coordinate in any event view.
 * @param doc Current document used for DOM lookup and range creation.
 * @param x Client X coordinate
 * @param y Client Y coordinate
 * @param json The current event JSON
 * @returns Resolved action target data for the clicked value.
 */
const resolveEventValueTargetAtPoint = (
	doc: Document,
	x: number,
	y: number,
	json: Record<string, unknown> | null,
): ResolvedEventValueTarget | null => {
	const element = doc.elementFromPoint(x, y) as HTMLElement;
	if (!element) return null;

	// 1. Table Handling
	if (element.closest(`.${s.tableView}`)) {
		return resolveTableEventValueTarget(doc, element);
	}

	// 3. Raw Handling (check before tree to ensure proper precedence)
	if (element.closest(`.${s.highlighter}`)) {
		return resolveRawEventValueTarget(doc, element, x, y);
	}

	// 2. Tree Handling - check multiple ways to identify tree context
	// This includes direct node/container checks plus fallback for nested elements
	const inTreeContainer =
		element.closest(`.${s.container}`) ?? element.closest(`.${s.node}`);
	const inScrollableTree =
		element.closest(`.${s.scrollable}`)?.querySelector(`.${s.container}`) !==
			null ||
		element.closest(`.${s.scrollable}`)?.querySelector(`.${s.node}`) !== null;

	if (
		inTreeContainer ||
		(element.closest(`.${s.scrollable}`) && inScrollableTree)
	) {
		return resolveTreeEventValueTarget(doc, element, json);
	}

	return null;
};

/**
 * Reads the current browser selection as an action value.
 * @param doc Current document that owns the event dialog.
 * @returns Trimmed selected text, or an empty string when no text is selected.
 */
const getCurrentSelectedText = (doc: Document): string => {
	return doc.defaultView?.getSelection()?.toString().trim() ?? "";
};

/**
 * Applies a DOM range as the current browser selection.
 * @param doc Current document that owns the selection.
 * @param range Range to select.
 */
const selectRange = (doc: Document, range: Range): void => {
	const selection = doc.defaultView?.getSelection();
	if (!selection) return;
	selection.removeAllRanges();
	selection.addRange(range);
};

/**
 * Builds the event action target from a right-click position and optional manual selection.
 * @param doc Current document that owns the event dialog.
 * @param x Client X coordinate.
 * @param y Client Y coordinate.
 * @param json The current event JSON.
 * @returns Action target for context-menu actions, or null when nothing selectable was found.
 */
const resolveEventActionTarget = (
	doc: Document,
	x: number,
	y: number,
	json: Record<string, unknown> | null,
): EventActionTarget | null => {
	const selectedText = getCurrentSelectedText(doc);
	const isManualSelection =
		selectedText.length > 0 && isPointInSelection(doc, x, y);
	const resolvedTarget = resolveEventValueTargetAtPoint(doc, x, y, json);

	if (isManualSelection) {
		return {
			...(resolvedTarget ?? {}),
			source: "manual",
			value: selectedText,
			displayText: selectedText,
		};
	}

	if (!resolvedTarget) {
		return null;
	}

	if (resolvedTarget.selectionRange) {
		selectRange(doc, resolvedTarget.selectionRange);
	}

	return {
		...resolvedTarget,
		source: "auto",
	};
};

/**
 * Builds a { key: value } object from selected cells in the table view.
 * Returns null when the current selection does not intersect the event table.
 */
/**
 * Builds a { key: value } object from selected cells in the table view.
 * Returns null when the current selection does not intersect the event table.
 */
const getSelectedTableKeyValueObject = (
	doc: Document,
): Record<string, string> | null => {
	const selection = doc.defaultView?.getSelection();
	if (!selection || selection.rangeCount === 0 || selection.isCollapsed) {
		return null;
	}

	const range = selection.getRangeAt(0);
	const selectedCells = Array.from(
		doc.querySelectorAll(`.${s.tableView} tbody td`),
	).filter((cell) => {
		try {
			return range.intersectsNode(cell);
		} catch {
			return false;
		}
	}) as HTMLTableCellElement[];

	if (selectedCells.length === 0) {
		return null;
	}

	const table = selectedCells[0].closest("table");
	if (!table) {
		return null;
	}

	const headerCells = Array.from(table.querySelectorAll("thead th")).map(
		(header) => header.textContent?.trim().toLowerCase() ?? "",
	);

	let keyIndex = headerCells.indexOf("key");
	let valueIndex = headerCells.indexOf("value");

	if (keyIndex < 0 || valueIndex < 0) {
		const rowCellCount = selectedCells[0].closest("tr")?.cells.length ?? 0;
		if (rowCellCount >= 2) {
			keyIndex = 0;
			valueIndex = 1;
		} else {
			return null;
		}
	}

	const output: Record<string, string> = {};
	const rows = new Set(
		selectedCells
			.map((cell) => cell.closest("tr"))
			.filter(
				(row): row is HTMLTableRowElement => row instanceof HTMLTableRowElement,
			),
	);

	rows.forEach((row) => {
		const key = row.cells[keyIndex]?.textContent?.trim();
		const value = row.cells[valueIndex]?.textContent?.trim();

		if (!key || key === "" || value == null) {
			return;
		}

		// Strip array indices from the key
		const cleanedKey = stripArrayIndices(key);
		output[cleanedKey] = value;
	});

	return Object.keys(output).length > 0 ? output : null;
};

/**
 * Dialog component for displaying and interacting with a single event's details.
 * Supports Tree, Raw (JSON), and Table views with rich context menu actions.
 */
export function DisplayEventDialog({
	event,
	onClose,
}: {
	event: Doc.Type;
	onClose?: () => void;
}) {
	if (!event) return null;

	const {
		Info,
		app,
		spawnBanner,
		spawnDialog,
		currentDocument,
		detachedEventDetail,
		requestMainEventAction,
	} = Application.use();
	const { extensions } = Extension.use();
	const { t } = Locale.use();

	// --- STATE ---
	const [transformsVersion, setTransformsVersion] = useState<number>(0);
	const [hiddenTransformFields, setHiddenTransformFields] = useState<
		Set<string>
	>(() => new Set());
	const [eventActionTarget, setEventActionTarget] =
		useState<EventActionTarget | null>(null);
	const [isFlagged, setIsFlagged] = useState(() =>
		getEventFlagState(app, event),
	);
	const [collabPanelHeight, setCollabPanelHeight] = useState<number>(
		INITIAL_COLLAB_PANEL_HEIGHT,
	);
	const [linkedEventsPopup, setLinkedEventsPopup] = useState<{
		events: Doc.Type[];
		anchor: { x: number; y: number };
	} | null>(null);
	const treeContextPathRef = useRef<string | null>(null);
	const treeContainerRef = useRef<HTMLDivElement | null>(null);
	const { theme } = useTheme();
	const [eventDetailView, selectEventDetailView] =
		useEventDetailViewPreference(currentDocument);
	const dataStoreVersion = useSyncExternalStore(
		DataStore.subscribeOverlays,
		DataStore.getOverlaysSnapshot,
	);

	// --- DERIVED DATA ---
	const notes = useMemo(
		() => Doc.Entity.notes(app, event),
		[dataStoreVersion, app.timeline.renderVersion, event],
	);
	const links = useMemo(
		() => Doc.Entity.links(app, event),
		[dataStoreVersion, app.timeline.renderVersion, event],
	);
	const file = useMemo(
		() => Source.Entity.id(app, event["gulp.source_id"]),
		[app.target.files, event],
	);
	const eventOperationId =
		file?.operation_id ??
		(
			event as Doc.Type & {
				"gulp.operation_id"?: Operation.Id;
			}
		)["gulp.operation_id"];
	const {
		json,
		loading: detailLoading,
		error: detailRequestError,
		setJson: setJSON,
		retry: retryDetail,
	} = useEventDetail({
		eventId: event._id,
		operationId: eventOperationId,
		query: Info.query_single_id,
		prepare: prepareEventJson,
	});
	const detailError = detailRequestError !== null;

	/**
	 * Delegates a mutating Event workflow to the main document when this Dialog
	 * is independently rendered.
	 * @param action Serializable workflow descriptor resolved by the main coordinator.
	 * @returns True when the detached request was accepted; otherwise false.
	 */
	const requestEventActionInMain = useCallback(
		(action: WindowBridge.EventActionRequest): boolean =>
			requestMainEventAction({ eventId: event._id, action }),
		[event._id, requestMainEventAction],
	);

	// --- EFFECTS ---

	/** Keeps the timeline crosshair aligned with the event visible in this dialog. */
	useEffect(() => {
		const nextTarget = file ? event : null;
		const currentTargetId = app.timeline.target?._id ?? null;
		const nextTargetId = nextTarget?._id ?? null;
		if (currentTargetId === nextTargetId) return;
		Info.setTimelineTarget(nextTarget);
	}, [app.timeline.target?._id, event, file, Info]);

	useEffect(() => {
		setIsFlagged(getEventFlagState(app, event));
	}, [app, event]);

	/**
	 * Keeps the notes/links panel bounded when the dialog opens or the viewport changes.
	 */
	useEffect(() => {
		const localWin = currentDocument.defaultView ?? window;

		/**
		 * Re-clamps the collaboration panel height against the current window bounds.
		 */
		const handleWindowResize = () => {
			setCollabPanelHeight((currentHeight) =>
				clampCollabPanelHeight(currentHeight, localWin.innerHeight),
			);
		};

		handleWindowResize();
		localWin.addEventListener("resize", handleWindowResize);

		return () => localWin.removeEventListener("resize", handleWindowResize);
	}, [currentDocument]);

	useEffect(() => setEventActionTarget(null), [event._id]);
	useEffect(() => setLinkedEventsPopup(null), [event._id]);
	useEffect(() => setHiddenTransformFields(new Set()), [event._id]);

	useEffect(() => {
		if (detachedEventDetail?._id !== event._id) return;
		setJSON(
			detachedEventDetail as unknown as Record<string, unknown>,
		);
	}, [detachedEventDetail, event._id, setJSON]);

	useEffect(
		() => EventJsonTransforms.subscribe(() => setTransformsVersion((v) => v + 1)),
		[],
	);

	const transformResult = useMemo(() => {
		void transformsVersion;
		if (!json) return null;
		if (!EventJsonTransforms.hasAny()) {
			return { json, transformedFields: new Set<string>() };
		}
		return EventJsonTransforms.apply(
			event["gulp.source_id"],
			json,
			hiddenTransformFields,
		);
	}, [json, hiddenTransformFields, transformsVersion, event]);

	const displayJson = transformResult?.json ?? null;

	const transformedFields = useMemo(
		() =>
			transformResult ? Array.from(transformResult.transformedFields) : [],
		[transformResult],
	);

	const suppressedFields = useMemo(
		() => Array.from(hiddenTransformFields),
		[hiddenTransformFields],
	);

	const toggleTransformField = useCallback((field: string) => {
		setHiddenTransformFields((prev) => {
			const next = new Set(prev);
			if (next.has(field)) next.delete(field);
			else next.add(field);
			return next;
		});
	}, []);

	// --- HANDLERS: Actions ---

	const handleCopyJson = useCallback(() => {
		if (!json) return;
		const path = treeContextPathRef.current;
		if (!path) {
			const selectedTableJson = getSelectedTableKeyValueObject(currentDocument);
			if (selectedTableJson) {
				copy(JSON.stringify(selectedTableJson, null, 2));
				return;
			}
			copy(JSON.stringify(json, null, 2));
			return;
		}

		const value = getValueByPath(json, path);
		if (value !== undefined) {
			copy(JSON.stringify({ [path]: value }, null, 2));
		} else {
			copy(JSON.stringify(json, null, 2));
		}
	}, [currentDocument, json]);

	const handleDownloadJson = useCallback(() => {
		if (json) {
			download(
				JSON.stringify(json, null, 2),
				"application/json",
				`${event._id}_from_${event["gulp.source_id"]}.json`,
			);
		}
	}, [json, event._id, event]);

	const handleDownloadLogFile = useCallback(() => {
		const storageId = json?.["gulp.storage_id"];
		const opId = Doc.Entity.operationId(app, event);
		if (
			storageId &&
			typeof storageId === "string" &&
			storageId.trim() !== "" &&
			opId
		) {
			Info.download_storage_file(storageId, opId);
		}
	}, [event, app, Info, json]);

	const handleFocusTimeline = useCallback(() => {
		// @ts-ignore
		return window.focusCanvasOnEvent(
			event.gulp_timestamp + (file?.settings.offset || 0),
			false,
			event["gulp.source_id"],
		);
	}, [event, file]);

	/**
	 * Closes the event dialog and clears the timeline target.
	 */
	const handleCloseDialog = useCallback(() => {
		onClose?.();
		spawnDialog(null);
		Info.setTimelineTarget(null);
	}, [Info, onClose, spawnDialog]);

	/**
	 * Toggles the current event flag state when an operation is available.
	 */
	const handleToggleFlag = useCallback(() => {
		const opId = Doc.Entity.operationId(app, event);
		if (!opId) return;
		setIsFlagged(Doc.Entity.flag.toggle(event._id, opId));
	}, [app, event]);

	/**
	 * Determines whether the event can be flagged under the current operation limit.
	 */
	const isFlagButtonDisabled = useMemo(() => {
		const opId = Doc.Entity.operationId(app, event);
		return (
			(opId &&
				Doc.Entity.flag.isLimitReached(Doc.Entity.flag.getList(opId)) &&
				!isFlagged) ||
			!opId
		);
	}, [app, event, isFlagged]);

	/**
	 * Checks whether at least one send-data extension is available for the action menu.
	 */
	const hasSendDataExtension = useMemo(
		() => Extension.hasSlot(extensions, Extension.Slot.SendData),
		[extensions],
	);

	/**
	 * Resolves plugin actions that should be mounted in the event action menu.
	 */
	const eventActionPlugins = useMemo(
		() => Extension.getBySlot(extensions, Extension.Slot.EventActions),
		[extensions],
	);

	// --- HANDLERS: Banners ---

	const handleEnrich = useCallback(
		(enrichmentField?: { key: string; value: string }) => {
			if (
				requestEventActionInMain({
					kind: "enrich",
					field: enrichmentField,
				})
			) {
				return;
			}
			spawnBanner(
				<Enrichment.Banner
					event={event}
					enrichmentField={enrichmentField}
					onEnrichment={(e) =>
						setJSON(e as unknown as Record<string, unknown>)
					}
				/>,
			);
		},
		[spawnBanner, event, requestEventActionInMain, setJSON],
	);

	const handleCreateNote = useCallback(
		(initialText?: string) => {
			if (
				requestEventActionInMain({
					kind: "note-create",
					initialText,
				})
			) {
				return;
			}
			spawnBanner(
				<NoteFunctionality.Create.Banner
					event={event}
					note={initialText ? ({ text: initialText } as Note.Type) : undefined}
				/>,
			);
		},
		[event, requestEventActionInMain, spawnBanner],
	);

	const handleCreateLink = useCallback(() => {
		if (requestEventActionInMain({ kind: "link-create" })) return;
		spawnBanner(
			<LinkFunctionality.Create.Banner
				event={event}
				initialDocIds={[event._id]}
			/>,
		);
	}, [spawnBanner, event, requestEventActionInMain]);

	const handleSendData = useCallback(() => {
		if (requestEventActionInMain({ kind: "send-data" })) return;
		spawnBanner(<SendData.Banner event={event} />);
	}, [spawnBanner, event, requestEventActionInMain]);

	const handleConnectLink = useCallback(() => {
		if (requestEventActionInMain({ kind: "link-connect" })) return;
		spawnBanner(<LinkFunctionality.Connect.Banner event={event} />);
	}, [spawnBanner, event, requestEventActionInMain]);

	/**
	 * Opens every event connected to the provided link.
	 *
	 * @param link The link whose connected events should be displayed.
	 * @param triggerEvent Mouse event used to anchor the floating event list.
	 */
	const handleOpenLinkedEvents = useCallback(
		async (
			link: Link.Type,
			triggerEvent: ReactMouseEvent<HTMLButtonElement>,
		): Promise<void> => {
			const rect = triggerEvent.currentTarget.getBoundingClientRect();
			const linkedEvents = await getRelatedLinkEvents(link);

			if (linkedEvents.length === 0) return;

			setLinkedEventsPopup({
				events: linkedEvents,
				anchor: {
					x: rect.left,
					y: rect.top,
				},
			});
		},
		[],
	);

	/**
	 * Opens the link action banner for disconnect/delete decisions.
	 *
	 * @param link The selected link table row.
	 */
	const handleOpenLinkActionBanner = useCallback(
		(link: Link.Type) => {
			if (
				requestEventActionInMain({
					kind: "link-manage",
					linkId: link.id,
				})
			) {
				return;
			}
			spawnBanner(
				<LinkEventActionBanner
					event={event}
					link={link}
				/>,
			);
		},
		[event, requestEventActionInMain, spawnBanner],
	);

	/**
	 * Opens the filter banner with the current action target as a single equality filter.
	 * @param target Selection target resolved from the event viewer context menu.
	 */
	const applyActionTargetAsFileFilter = useCallback(
		(target: EventActionTarget | null) => {
			if (!target?.field) {
				toast(t("eventDialog.invalidFilterSelection"));
				return;
			}
			if (
				requestEventActionInMain({
					kind: "filter",
					field: target.field,
					value: target.value,
				})
			) {
				return;
			}

			const sourceFile = Source.Entity.id(app, event["gulp.source_id"]);
			const { filters } = Info.getQuery(sourceFile);

			const newFilter = Filter.Entity.createCondition(
				generateUUID<Filter.Id>(),
				{
					field: target.field,
					value: target.value,
				},
			);

			const updatedQuery = {
				...Info.getQuery(sourceFile),
				filters: [...filters, newFilter],
			};

			spawnBanner(
				<FilterFileBanner
					sources={[sourceFile]}
					query={updatedQuery}
				/>,
			);
		},
		[Info, app, event, requestEventActionInMain, spawnBanner, t],
	);

	// --- UI COMPONENTS: Sub-renders ---

	const highlights = useMemo(() => {
		if (!displayJson) return null;

		const storageId = displayJson["gulp.storage_id"];
		const tableRows = flattenTableEntries(displayJson).sort((a, b) =>
			a.key.localeCompare(b.key),
		);
		const unflattenObject = sortTreeValueRecursively(
			Object.keys(displayJson).reduce((res, k) => {
				k.split(".").reduce(
					(acc: any, e, i, keys) =>
						acc[e] ||
						(acc[e] = isNaN(Number(keys[i + 1]))
							? keys.length - 1 === i
								? displayJson[k]
								: {}
							: []),
					res,
				);
				return res;
			}, {}),
		) as Record<string, unknown>;

		const baseJsonStyles =
			theme === "light" || theme === "light-old" ? defaultStyles : darkStyles;

		const jsonStyles: StyleProps = {
			...baseJsonStyles,
			noQuotesForStringValues: false,
			quotesForFieldNames: true,
			stringifyStringValues: true,
			basicChildStyle: s.node,
			childFieldsContainer: s.basic,
			clickableLabel: s.clickableLabel,
			punctuation: s.punctuation,
			stringValue: s.string,
			numberValue: s.numeric,
			booleanValue: s.bool,
			nullValue: s.null,
			undefinedValue: s.undefined,
			otherValue: s.other,
			expandIcon: s.expandIcon,
			collapseIcon: s.collapseIcon,
			collapsedContent: s.collapsed,
			container: s.container,
			label: s.label,
		};
		const eventActionText =
			eventActionTarget?.source === "auto"
				? eventActionTarget.displayText
				: eventActionTarget?.value;

		return (
			<ContextMenu>
				<Tabs
					value={eventDetailView}
					onValueChange={selectEventDetailView}
					className={s.tabs_wrapper}
				>
					<TabsList className={s.triggers}>
						<TabsTrigger value="raw">
							<Icon
								name="CodeBracket"
								size={14}
							/>{" "}
							{t("eventDialog.raw")}
						</TabsTrigger>
						<TabsTrigger value="table">
							<Icon
								name="Table"
								size={14}
							/>{" "}
							{t("eventDialog.table")}
						</TabsTrigger>
						<TabsTrigger value="tree">
							<Icon
								name="GitFork"
								size={14}
							/>{" "}
							{t("eventDialog.tree")}
						</TabsTrigger>
					</TabsList>

					<ContextMenuTrigger
						asChild
						onMouseDown={(e) => {
							if (e.button === 2) {
								const target = resolveEventActionTarget(
									currentDocument,
									e.clientX,
									e.clientY,
									json,
								);
								setEventActionTarget(target);
								treeContextPathRef.current =
									target?.view === "tree" && target.fieldPath
										? target.fieldPath
										: null;

								if (!target) {
									currentDocument.defaultView
										?.getSelection()
										?.removeAllRanges();
								}
							}
						}}
					>
						<div
							className={s.contextTrigger}
							ref={treeContainerRef}
						>
							<TabsContent
								value="tree"
								className={s.scrollable}
							>
								<JsonView
									data={unflattenObject}
									clickToExpandNode={false}
									shouldExpandNode={allExpanded}
									style={jsonStyles}
								/>
							</TabsContent>
							<TabsContent
								className={s.scrollable}
								value="raw"
							>
								<Markdown
									className={s.highlighter}
									value={`\`\`\`json\n${JSON.stringify(displayJson, null, 2)}`}
								/>
							</TabsContent>
							<TabsContent
								value="table"
								className={s.scrollable}
							>
								<Table
									className={s.tableView}
									includeIndex={false}
									values={tableRows}
								/>
							</TabsContent>
						</div>
					</ContextMenuTrigger>
				</Tabs>

				<ContextMenuContent container={currentDocument.body}>
					<ContextMenuItem
						disabled={!eventActionText}
						onClick={() => handleCreateNote(eventActionText)}
						icon="StickyNote"
					>
						{t("eventDialog.createNewNote")}
					</ContextMenuItem>
					<ContextMenuItem
						disabled={!eventActionTarget}
						onClick={handleCreateLink}
						icon="GitPullRequestCreate"
					>
						{t("eventDialog.createNewLink")}
					</ContextMenuItem>
					<ContextMenuItem
						disabled={!eventActionText}
						onClick={() => {
							if (eventActionText) copy(eventActionText);
						}}
						icon="Copy"
					>
						{t("common.copy")}
					</ContextMenuItem>
					<ContextMenuItem
						onClick={() => applyActionTargetAsFileFilter(eventActionTarget)}
						icon="Filter"
					>
						{t("eventDialog.newFilter")}
					</ContextMenuItem>
					<ContextMenuItem
						disabled={!eventActionTarget}
						onClick={() => {
							if (!eventActionTarget) return;
							handleEnrich({
								key: eventActionTarget.field ?? "selection",
								value: eventActionTarget.value,
							});
						}}
						icon="PrismColor"
					>
						{t("targetMenu.enrich")}
					</ContextMenuItem>
					{typeof storageId === "string" &&
						storageId.trim() !== "" && (
							<ContextMenuItem
								onClick={handleDownloadLogFile}
								icon="Download"
							>
								{t("eventDialog.downloadLogFile")}
							</ContextMenuItem>
						)}
					<Extension.Components
						type={Extension.Slot.EventContextMenu}
						props={{
							slot: Extension.Slot.EventContextMenu,
							event,
							target: eventActionTarget,
							sourceId: event["gulp.source_id"],
						}}
					/>
				</ContextMenuContent>
			</ContextMenu>
		);
	}, [
		displayJson,
		eventActionTarget,
		spawnBanner,
		event,
		handleCreateNote,
		applyActionTargetAsFileFilter,
		handleDownloadLogFile,
		handleEnrich,
		handleCreateLink,
		currentDocument,
		eventDetailView,
		selectEventDetailView,
		t,
	]);

	/**
	 * Converts connected links into the table shape used by the lower links tab.
	 */
	const linkTableRows = useMemo<EventLinkTableRow[]>(
		() =>
			links.map((link) => ({
				id: link.id,
				_id: link.id,
				name: link.name,
				description: link.description,
				color: link.color,
				icon: Link.Entity.icon(link),
			})),
		[links],
	);

	/**
	 * Defines the visible columns and custom link-name renderer for the links tab.
	 */
	const linkTableColumns = useMemo<Table.ColumnDefinition<EventLinkTableRow>[]>(
		() => [
			{
				key: "name",
				label: t("common.name"),
				width: 180,
				render: (_value, row) => (
					<span
						className={s.linkName}
						style={{ color: row.color }}
					>
						<Icon
							name={row.icon}
							color={row.color}
							size={12}
						/>
						{row.name}
					</span>
				),
			},
			{
				key: "description",
				label: t("common.description"),
				width: "auto",
			},
		],
		[t],
	);

	/**
	 * Defines the per-row actions available for connected links.
	 */
	const linkTableActions = useMemo<Table.Action<EventLinkTableRow>[]>(
		() => [
			{
				icon: "MagnifyingGlassSmall",
				label: t("eventDialog.showLinkedEvents"),
				onClick: (_row, index, actionEvent) => {
					const link = links[index];
					if (link) handleOpenLinkedEvents(link, actionEvent);
				},
			},
			{
				icon: "Trash2",
				label: t("eventDialog.manageLink"),
				onClick: (_row, index) => {
					const link = links[index];
					if (link) handleOpenLinkActionBanner(link);
				},
			},
		],
		[handleOpenLinkActionBanner, handleOpenLinkedEvents, links, t],
	);

	/**
	 * Starts vertical resizing for the notes/links panel and updates its height while dragging.
	 *
	 * @param resizeEvent Mouse event emitted by the panel resize handle.
	 */
	const handleCollabResizeStart = useCallback(
		(resizeEvent: ReactMouseEvent<HTMLDivElement>) => {
			resizeEvent.preventDefault();

			const localDoc = resizeEvent.currentTarget.ownerDocument;
			const localWin = localDoc.defaultView ?? window;
			const startY = resizeEvent.clientY;
			const startHeight = collabPanelHeight;

			const handleMouseMove = (moveEvent: MouseEvent) => {
				const nextHeight = startHeight + startY - moveEvent.clientY;
				setCollabPanelHeight(
					clampCollabPanelHeight(nextHeight, localWin.innerHeight),
				);
			};

			const handleMouseUp = () => {
				localWin.removeEventListener("mousemove", handleMouseMove);
				localWin.removeEventListener("mouseup", handleMouseUp);
				localDoc.body.style.cursor = "";
				localDoc.body.style.userSelect = "";
			};

			localDoc.body.style.cursor = "row-resize";
			localDoc.body.style.userSelect = "none";
			localWin.addEventListener("mousemove", handleMouseMove);
			localWin.addEventListener("mouseup", handleMouseUp);
		},
		[collabPanelHeight],
	);

	if (!file) {
		return (
			<Dialog
				callback={onClose}
				className={s.dialog}
			>
				<Stack
					style={{ width: "100%", height: "300px" }}
					flex
					ai="center"
					jc="center"
					dir="column"
					gap={12}
				>
					<Icon
						name="CircleAlert"
						size={48}
						style={{ color: "var(--red-500)" }}
					/>
					<p style={{ color: "var(--red-500)", fontWeight: "bold" }}>
						{t("eventDialog.sourceDeleted")}
					</p>
					<p style={{ opacity: 0.6 }}>{t("eventDialog.eventUnavailable")}</p>
				</Stack>
			</Dialog>
		);
	}

	return (
		<Dialog
			callback={onClose}
			className={s.dialog}
		>
			<Navigation event={event} />
			<div className={s.header}>
				<div className={s.titleContainer}>
					<div className={s.iconWrapper}>
						<Icon
							name="Triangle"
							size={18}
						/>
					</div>
					<h2 className={s.title}>{t("common.event")}</h2>
				</div>
				<div className={s.buttonGroup}>
					<Button
						variant="tertiary"
						className={s.menuAction}
						icon="StickyNote"
						onClick={() => handleCreateNote()}
						title={t("eventDialog.createNewNote")}
					></Button>
					<Button
						onClick={handleToggleFlag}
						variant="secondary"
						icon={isFlagged ? "FlagOff" : "Flag"}
						disabled={isFlagButtonDisabled}
						title={t("eventDialog.flagEvent")}
					/>
					<Button
						onClick={handleFocusTimeline}
						variant="secondary"
						icon="Crosshair"
						title={t("eventDialog.focusTimeline")}
					/>
					<Popover.Root>
						<Popover.Trigger asChild>
							<Button
								variant="secondary"
								icon="List"
								title={t("common.actions")}
							/>
						</Popover.Trigger>
						<Popover.Content
							className={s.actionMenu}
							container={currentDocument.body}
							align="end"
						>
							<Button
								variant="tertiary"
								className={s.menuAction}
								icon="GitPullRequestCreate"
								onClick={handleCreateLink}
							>
								{t("eventDialog.createAndConnectLink")}
							</Button>
							<Button
								variant="tertiary"
								className={s.menuAction}
								icon="GitPullRequestCreateArrow"
								onClick={handleConnectLink}
							>
								{t("eventDialog.connectLink")}
							</Button>
							<Button
								variant="tertiary"
								className={s.menuAction}
								icon="PrismColor"
								onClick={() => handleEnrich()}
							>
								{t("targetMenu.enrich")}
							</Button>
							{hasSendDataExtension && (
								<Button
									variant="tertiary"
									className={s.menuAction}
									icon="Send"
									onClick={handleSendData}
								>
									{t("eventDialog.sendIoc")}
								</Button>
							)}
							{eventActionPlugins.map((plugin) => (
								<Extension.Component
									className={s.pluginMenuAction}
									key={plugin.filename}
									name={plugin.filename}
									props={{ doc: event, event }}
								/>
							))}
							<Button
								variant="tertiary"
								className={s.menuAction}
								icon="Download"
								onClick={handleDownloadJson}
							>
								{t("eventDialog.downloadJson")}
							</Button>
						</Popover.Content>
					</Popover.Root>
					<Button
						variant="secondary"
						icon="X"
						title={t("common.closeDialog")}
						onClick={handleCloseDialog}
					/>
				</div>
			</div>

			{(transformedFields.length > 0 || suppressedFields.length > 0) && (
				<div className={s.transformBar}>
					<Icon
						name="Utensils"
						size={12}
						color="var(--gray-700)"
					/>
					<span className={s.transformBarLabel}>Transformed:</span>
					{transformedFields.map((field) => (
						<button
							key={`on-${field}`}
							type="button"
							className={cn(s.transformChip, s.transformChipOn)}
							onClick={() => toggleTransformField(field)}
							title="Showing transformed value — click to show raw"
						>
							<Icon name="Eye" size={10} />
							<code>{field}</code>
						</button>
					))}
					{suppressedFields.map((field) => (
						<button
							key={`off-${field}`}
							type="button"
							className={cn(s.transformChip, s.transformChipOff)}
							onClick={() => toggleTransformField(field)}
							title="Showing raw value — click to show transformed"
						>
							<Icon name="EyeOff" size={10} />
							<code>{field}</code>
						</button>
					))}
				</div>
			)}
			<div className={s.eventViewer}>
				<Button
					className={s.copyJsonButton}
					variant="secondary"
					onClick={handleCopyJson}
					icon="Copy"
					title={t("eventDialog.copyJson")}
					disabled={!json}
				/>
				{json ? (
					highlights
				) : detailError ? (
					<Stack
						style={{ width: "100%", height: "100%" }}
						flex
						ai="center"
						jc="center"
						dir="column"
						gap={12}
					>
						<Icon
							name="CircleAlert"
							size={40}
							style={{ color: "var(--red-500)" }}
						/>
						<p>{t("eventDialog.eventUnavailable")}</p>
						<Button
							variant="secondary"
							icon="RotateCw"
							onClick={retryDetail}
						>
							{t("common.refresh")}
						</Button>
					</Stack>
				) : detailLoading ? (
					<LoadingSkeleton />
				) : null}
			</div>

			<Tabs
				defaultValue="notes"
				className={s.collabTabs}
				style={
					{
						"--collab-panel-height": `${collabPanelHeight}px`,
					} as CSSProperties
				}
			>
				<div
					className={s.collabResizeHandle}
					onMouseDown={handleCollabResizeStart}
					role="separator"
					aria-orientation="horizontal"
				/>
				<div className={s.collabTabsHeader}>
					<TabsList className={s.triggers}>
						<TabsTrigger value="notes">
							<Icon
								name="StickyNote"
								size={14}
							/>{" "}
							{t("notes.title")}
						</TabsTrigger>
						<TabsTrigger value="links">
							<Icon
								name="GitPullRequestCreate"
								size={14}
							/>{" "}
							{t("eventDialog.links")}
						</TabsTrigger>
					</TabsList>
				</div>
				<div className={s.collabPanel}>
					<TabsContent
						value="notes"
						className={s.collabContent}
					>
						{notes.length > 0 ? (
							<Collab.List
								notes={notes}
								links={[]}
								container={currentDocument.body}
							/>
						) : (
							<div className={s.emptyState}>{t("eventDialog.noNotes")}</div>
						)}
					</TabsContent>
					<TabsContent
						value="links"
						className={s.collabContent}
					>
						{linkTableRows.length > 0 ? (
							<Table
								className={s.collabTable}
								values={linkTableRows}
								columns={linkTableColumns}
								includeIndex={false}
								actions={linkTableActions}
							/>
						) : (
							<div className={s.emptyState}>{t("eventDialog.noLinks")}</div>
						)}
					</TabsContent>
				</div>
			</Tabs>
			{linkedEventsPopup && (
				<DisplayGroupDialog
					events={linkedEventsPopup.events}
					anchor={linkedEventsPopup.anchor}
					groupKind="links"
					onClose={() => setLinkedEventsPopup(null)}
					preserveTimelineTarget
				/>
			)}
		</Dialog>
	);
}

/**
 * Skeleton loader for the dialog.
 */
function LoadingSkeleton() {
	return (
		<Stack
			style={{ width: "100%", height: "100%" }}
			flex
			ai="center"
			jc="center"
			dir="column"
			gap={12}
		>
			<Stack style={{ width: "100%" }}>
				<Skeleton width="full" />
				<Skeleton width="full" />
			</Stack>
			<Stack style={{ width: "100%" }}>
				<Skeleton width="full" />
				<Skeleton width="full" />
			</Stack>
			<Skeleton
				width="full"
				height="full"
			/>
			<Stack style={{ width: "100%" }}>
				<Skeleton width="full" />
				<Skeleton width="full" />
				<Skeleton width="full" />
			</Stack>
		</Stack>
	);
}

/**
 * Banner that lets the user choose whether to disconnect the current event
 * from a link or delete the link entirely.
 */
export function LinkEventActionBanner({
	event,
	link,
}: {
	event: Doc.Type;
	link: Link.Type;
}) {
	const { Info, destroyBanner, spawnBanner } = Application.use();
	const { t } = Locale.use();
	const [loading, setLoading] = useState<boolean>(false);

	/**
	 * Disconnects the current event from the selected link.
	 */
	const handleDisconnectEvent = async () => {
		setLoading(true);
		await Info.links_disconnect(link, event);
		setLoading(false);
		destroyBanner();
		toast(t("link.eventDisconnected", { event: event._id, link: link.name }));
	};

	/**
	 * Replaces this choice banner with the existing delete-link confirmation.
	 */
	const handleDeleteLink = () => {
		spawnBanner(<Link.Delete.Banner link={link} />);
	};

	return (
		<UIBanner title={t("eventDialog.manageLink")}>
			<p>
				{t("eventDialog.manageLinkDescription")} <code>{link.name}</code>
			</p>
			<Stack
				dir="column"
				ai="stretch"
				gap={8}
			>
				<Button
					loading={loading}
					variant="secondary"
					icon="Unlink"
					onClick={handleDisconnectEvent}
				>
					{t("eventDialog.removeEventFromLink")}
				</Button>
				<Button
					variant="destructive"
					icon="Trash2"
					onClick={handleDeleteLink}
				>
					{t("link.deleteTitle")}
				</Button>
			</Stack>
		</UIBanner>
	);
}

// --- SECONDARY COMPONENTS ---

export namespace EventIndicator {
	export interface Props extends Button.Props {
		event: Doc.Type;
		resolvedColor?: string;
		hasNotesOverride?: boolean;
		hasLinksOverride?: boolean;
	}
}

/**
 * Visual indicator button for events in lists or timelines.
 */
export function EventIndicator({
	event,
	className,
	style,
	resolvedColor,
	hasNotesOverride,
	hasLinksOverride,
	...props
}: EventIndicator.Props) {
	const { app, detachedDialogEventColors } = Application.use();
	if (!event) return null;

	const file = Source.Entity.id(app, event["gulp.source_id"]);
	const detachedColor = detachedDialogEventColors[event._id];

	const notes = useMemo(
		() => Doc.Entity.notes(app, event),
		[app.timeline.renderVersion, event._id],
	);
	const links = useMemo(
		() => Doc.Entity.links(app, event),
		[app.timeline.renderVersion, event._id],
	);

	const background = useMemo(() => {
		return (
			resolvedColor ??
			detachedColor ??
			resolveEventRenderColor(app, event) ??
			"var(--accent)"
		);
	}, [
		app,
		app.timeline.renderVersion,
		app.target.files,
		detachedColor,
		event,
		event.number_hash,
		file,
		resolvedColor,
	]);

	const indicatorPresentation = useMemo(
		() => getEventIndicatorPresentation(event),
		[event.indicatorValue, event["gulp.event_code"]],
	);
	const indicatorLabel = indicatorPresentation.label;
	const indicatorTooltip = indicatorPresentation.tooltip;
	const indicatorFontSize = useMemo(() => {
		return 10;
	}, [indicatorLabel]);

	return (
		<Button
			shape="icon"
			className={cn(className, s.indicator)}
			title={indicatorTooltip}
			aria-label={indicatorTooltip}
			style={{ ...style, "--event-color": background } as CSSProperties}
			{...props}
		>
			<Stack
				className={s.indicatorIconWrap}
				ai="center"
				jc="center"
				dir="column"
				gap={2}
			>
				<p
					style={{
						fontSize: `${indicatorFontSize}px`,
					}}
				>
					{indicatorLabel}
				</p>
			</Stack>
			{Doc.Entity.flag.isFlagged(
				event._id,
				Doc.Entity.operationId(app, event),
			) && (
				<Stack
					ai="center"
					jc="center"
					className={cn(s.marker, s.flagged)}
					pos="absolute"
				>
					<Icon
						size={8}
						name="Flag"
					/>
				</Stack>
			)}
			{(hasNotesOverride ?? notes.length > 0) && (
				<Stack
					ai="center"
					jc="center"
					className={cn(s.marker, s.collab)}
					pos="absolute"
				>
					<Icon
						size={8}
						name="StickyNote"
					/>
				</Stack>
			)}
			{(hasLinksOverride ?? links.length > 0) && (
				<Stack
					ai="center"
					jc="center"
					className={cn(s.marker, s.collab, s.linkMarker)}
					pos="absolute"
				>
					<Icon
						size={8}
						name="Link"
					/>
				</Stack>
			)}
		</Button>
	);
}
