import { useEffect, useState, type KeyboardEvent } from "react";
import { Button } from "@/ui/Button";
import { Input } from "@/ui/Input";
import { Select } from "@/ui/Select";
import { Locale } from "@/locales";
import s from "./styles/TablePagination.module.css";

const DEFAULT_PAGE_SIZE_OPTIONS = [20, 50, 100] as const;
const PAGE_INPUT_TOKEN = "__GULP_PAGE_INPUT__";

export namespace TablePagination {
	export interface Props {
		/** One-based active page. */
		currentPage: number;
		/** Number of entries across all pages. */
		totalItems: number;
		/** Number of entries requested for each page. */
		pageSize: number;
		/** Allowed values shown in the page-size selector. */
		pageSizeOptions?: readonly number[];
		/** Disables page navigation while the owning view is loading. */
		disabled?: boolean;
		/** Portal target for the page-size selector. */
		container?: HTMLElement | null;
		/** Receives a validated, one-based page number. */
		onPageChange: (page: number) => void;
		/** Receives the selected page size. */
		onPageSizeChange: (pageSize: number) => void;
	}
}

/**
 * Parses a page input only when it is an integer inside the available range.
 * @param value Raw value entered by the user.
 * @param maximumPage Highest page that can be selected.
 * @returns The valid page number, or `null` when the input is invalid.
 */
export function parseTablePageInput(
	value: string,
	maximumPage: number,
): number | null {
	if (!/^[1-9]\d*$/.test(value)) return null;

	const page = Number(value);
	const normalizedMaximumPage = Math.max(1, Math.floor(maximumPage));
	return Number.isSafeInteger(page) && page <= normalizedMaximumPage
		? page
		: null;
}

/**
 * Renders localized table range, page-size, editable page, and navigation controls.
 * @param props Pagination values and callbacks owned by the parent table view.
 * @returns Reusable pagination controls.
 */
export function TablePagination({
	currentPage,
	totalItems,
	pageSize,
	pageSizeOptions = DEFAULT_PAGE_SIZE_OPTIONS,
	disabled = false,
	container,
	onPageChange,
	onPageSizeChange,
}: TablePagination.Props) {
	const { t } = Locale.use();
	const normalizedPageSize = Math.max(1, Math.floor(pageSize));
	const normalizedTotalItems = Math.max(0, Math.floor(totalItems));
	const totalPages = Math.max(
		1,
		Math.ceil(normalizedTotalItems / normalizedPageSize),
	);
	const boundedCurrentPage = Math.min(
		totalPages,
		Math.max(1, Math.floor(currentPage)),
	);
	const [pageInput, setPageInput] = useState(
		boundedCurrentPage.toString(),
	);
	const parsedPageInput = parseTablePageInput(pageInput, totalPages);
	const localizedPageSummary = t("tableView.pageOf", {
		page: PAGE_INPUT_TOKEN,
		total: totalPages,
	});
	const [pageSummaryPrefix = "", pageSummarySuffix = ""] =
		localizedPageSummary.split(PAGE_INPUT_TOKEN);
	const pageAriaLabel = t("tableView.pageOf", {
		page: boundedCurrentPage,
		total: totalPages,
	});

	useEffect(() => {
		setPageInput(boundedCurrentPage.toString());
	}, [boundedCurrentPage]);

	/** Applies the draft page only after it passes all range checks. */
	const commitPageInput = (): void => {
		if (parsedPageInput === null) return;
		onPageChange(parsedPageInput);
	};

	/**
	 * Commits on Enter and restores the active page on Escape.
	 * @param event Keyboard event raised by the page input.
	 */
	const handlePageInputKeyDown = (
		event: KeyboardEvent<HTMLInputElement>,
	): void => {
		if (event.key === "Enter") {
			event.preventDefault();
			commitPageInput();
		} else if (event.key === "Escape") {
			event.preventDefault();
			setPageInput(boundedCurrentPage.toString());
		}
	};

	return (
		<div className={s.root}>
			<span className={s.label}>
				{t("tableView.showingRange", {
					from:
						normalizedTotalItems > 0
							? normalizedPageSize * (boundedCurrentPage - 1) + 1
							: 0,
					to: Math.min(
						normalizedPageSize * boundedCurrentPage,
						normalizedTotalItems,
					),
					total: normalizedTotalItems,
				})}
			</span>

			<div className={s.pageSizeControl}>
				<Select.Root
					disabled={disabled}
					value={normalizedPageSize.toString()}
					onValueChange={(value) => onPageSizeChange(Number(value))}
				>
					<Select.Trigger className={s.pageSizeSelect}>
						<Select.Value />
					</Select.Trigger>
					<Select.Content container={container}>
						{pageSizeOptions.map((size) => (
							<Select.Item
								key={size}
								value={size.toString()}
							>
								{size}
							</Select.Item>
						))}
					</Select.Content>
				</Select.Root>
				<span className={s.label}>{t("tableView.perPage")}</span>
			</div>

			<div className={s.pageEditor}>
				<span className={s.label}>{pageSummaryPrefix}</span>
				<div className={s.pageInputContainer}>
					<Input
						className={s.pageInput}
						disabled={disabled}
						type="number"
						inputMode="numeric"
						min={1}
						max={totalPages}
						step={1}
						value={pageInput}
						valid={parsedPageInput !== null}
						aria-invalid={parsedPageInput === null}
						aria-label={pageAriaLabel}
						onChange={(event) => setPageInput(event.target.value)}
						onBlur={commitPageInput}
						onKeyDown={handlePageInputKeyDown}
					/>
				</div>
				<span className={s.label}>{pageSummarySuffix}</span>
			</div>

			<div className={s.navigationButtons}>
				<Button
					variant="secondary"
					icon="ChevronLeft"
					disabled={boundedCurrentPage <= 1 || disabled}
					onClick={() =>
						onPageChange(Math.max(1, boundedCurrentPage - 1))
					}
					title={t("tableView.previousPage")}
					aria-label={t("tableView.previousPage")}
				/>
				<Button
					variant="secondary"
					icon="ChevronRight"
					disabled={boundedCurrentPage >= totalPages || disabled}
					onClick={() =>
						onPageChange(Math.min(totalPages, boundedCurrentPage + 1))
					}
					title={t("tableView.nextPage")}
					aria-label={t("tableView.nextPage")}
				/>
			</div>
		</div>
	);
}
