import { useEffect, useState } from "react";
import { Application } from "@/context/Application.context";
import { Operation } from "@/entities/Operation";
import { Dialog } from "@/ui/Dialog";
import { Badge } from "@/ui/Badge";
import { Button } from "@/ui/Button";
import { DeleteConfirmationBanner } from "@/ui/DeleteConfirmationBanner";
import { Icon } from "@/ui/Icon";
import { Stack } from "@/ui/Stack";
import { formatTimestampToReadableString } from "@/ui/utils";
import { GulpDataset } from "@/class/Info";
import s from "./styles/DisplayOperationDetailDialog.module.css";
import sFiles from "@/banners/styles/SelectFilesBanner.module.css";
import {
	ContextHeading,
	FlatFileComponent,
} from "@/banners/SelectFiles.banner";
import { OperationPermissions } from "@/banners/Permissions.banner";
import { cn } from "@/ui/utils";
import { Context } from "@/entities/Context";
import { Source } from "@/entities/Source";
import { Locale } from "@/locales";

export interface DisplayOperationDetailDialogProps {
	/** Unique identifier of the operation to fetch details for. */
	operationId: Operation.Id;
	/** Fallback name to show in the header while details are loading. */
	fallbackName?: string;
	/** Fallback glyph ID to show in the header while details are loading. */
	fallbackGlyphId?: string;
	/** Callback function triggered when the dialog should close. */
	onClose: () => void;
}

/**
 * Dialog component displaying comprehensive details of a specific operation,
 * including its configuration, owner, document counts, tags, and user/group permissions.
 * Wraps content in Gulp's native <Dialog> to support standard docking and resize behavior.
 */
export function DisplayOperationDetailDialog({
	operationId,
	fallbackName,
	fallbackGlyphId,
	onClose,
}: DisplayOperationDetailDialogProps) {
	const { app, Info, spawnBanner, destroyBanner } = Application.use();
	const { t } = Locale.use();
	const [loading, setLoading] = useState<boolean>(true);
	const [deleteLoading, setDeleteLoading] = useState<boolean>(false);
	const [details, setDetails] =
		useState<GulpDataset.OperationGetById.Response | null>(null);

	const globalOp = Info.app.target.operations.find(
		(op) => op.id === operationId,
	);

	// Fetch detailed operation data on mount or when operationId changes
	useEffect(() => {
		let active = true;

		const loadDetails = async () => {
			setLoading(true);
			try {
				const response = await Info.operation_get_by_id(operationId);
				if (active) {
					setDetails(response);
				}
			} catch (error) {
				console.error("Failed to load operation details:", error);
			} finally {
				if (active) {
					setLoading(false);
				}
			}
		};

		loadDetails();

		return () => {
			active = false;
		};
	}, [operationId, Info]);

	/**
	 * Formats a Unix timestamp to a human-readable date and time.
	 * Supports both seconds and milliseconds Unix formats.
	 *
	 * @param timestamp - The Unix timestamp to convert.
	 * @returns The formatted timestamp string.
	 */
	const formatUnixTimestamp = (timestamp: number): string => {
		const isSeconds = String(timestamp).length <= 10;
		const milliseconds = isSeconds ? timestamp * 1000 : timestamp;
		return formatTimestampToReadableString(milliseconds);
	};

	/**
	 * Opens the operation edit banner with locally available fallback details.
	 *
	 * @returns Nothing.
	 */
	const handleEdit = () => {
		const operation: Operation.Type = {
			id: operationId,
			index: globalOp?.index ?? "",
			name: details?.name ?? globalOp?.name ?? fallbackName ?? "",
			glyph_id:
				globalOp?.glyph_id ??
				((fallbackGlyphId ?? "") as Operation.Type["glyph_id"]),
			selected: globalOp?.selected ?? false,
			description:
				globalOp && globalOp.description !== undefined
					? globalOp.description
					: (details?.description ?? ""),
		};

		spawnBanner(
			<Operation.CreateOrUpdate.Banner
				operation={operation}
				onSuccess={async () => {
					try {
						const response = await Info.operation_get_by_id(operationId);
						setDetails(response);
					} catch (error) {
						console.error("Failed to reload operation details:", error);
					}
				}}
			/>,
		);
	};

	// Determine values to show in header depending on load state
	const displayName = details?.name ?? fallbackName ?? t("operationDetails.title");
	const fallbackOperation: Operation.Type | null = fallbackGlyphId
		? {
				id: operationId,
				index: "",
				name: displayName,
				glyph_id: fallbackGlyphId as Operation.Type["glyph_id"],
				selected: false,
			}
		: null;
	const iconName = Operation.Entity.icon(globalOp ?? fallbackOperation);

	/**
	 * Deletes the displayed operation and closes the detail dialog after success.
	 *
	 * @returns A promise resolving to false when deletion fails.
	 */
	const handleDelete = async (): Promise<boolean | void> => {
		const operationToDelete: Operation.Type =
			globalOp ??
			({
				id: operationId,
				index: "",
				name: displayName,
				glyph_id: (fallbackGlyphId ?? "") as Operation.Type["glyph_id"],
				selected: false,
			} satisfies Operation.Type);
		const deletedIds = await Info.deleteOperation(
			operationToDelete,
			setDeleteLoading,
		);
		if (deletedIds.length === 0) {
			return false;
		}
		onClose();
	};

	/**
	 * Opens the shared confirmation banner for deleting the displayed operation.
	 *
	 * @returns Nothing.
	 */
	const openDeleteBanner = () => {
		spawnBanner(
			<DeleteConfirmationBanner onConfirm={handleDelete}>
				<p>
					{t("common.delete")}: <code>{displayName}</code>
				</p>
			</DeleteConfirmationBanner>,
		);
	};

	return (
		<Dialog
			callback={onClose}
			loading={loading}
		>
			{/* Dialog Header */}
			<div className={s.header}>
				<div className={s.titleContainer}>
					<div className={s.iconWrapper}>
						<Icon
							name={iconName}
							size={18}
						/>
					</div>
					<h2 className={s.title}>{displayName}</h2>
				</div>
				<div className={s.buttonGroup}>
					<Button
						variant="secondary"
						icon="PencilEdit"
						title={t("operationDetails.edit")}
						onClick={handleEdit}
					/>
					<Button
						variant="secondary"
						icon="X"
						title={t("common.closeDialog")}
						onClick={onClose}
					/>
				</div>
			</div>

			{/* Dialog Body */}
			{details ? (
				<Stack
					dir="column"
					gap={20}
					className={s.scrollable}
				>
					{/* Details Section */}
					<div className={s.section}>
						<div className={s.detailsList}>
							<div className={s.detailItem}>
								<span className={s.detailLabel}>{t("common.id")}:</span>
								<span className={s.detailValue}>{details.id}</span>
							</div>
							<div className={s.detailItem}>
								<span className={s.detailLabel}>{t("common.name")}:</span>
								<span className={s.detailValue}>{details.name}</span>
							</div>
							<div className={s.detailItem}>
								<span className={s.detailLabel}>{t("common.created")}:</span>
								<span className={s.detailValue}>
									{formatUnixTimestamp(details.time_created)}
								</span>
							</div>
							<div className={s.detailItem}>
								<span className={s.detailLabel}>{t("common.owner")}:</span>
								<span className={s.detailValue}>{details.user_id}</span>
							</div>
							<div className={s.detailItem}>
								<span className={s.detailLabel}>{t("common.docCount")}:</span>
								<span className={s.detailValue}>
									{details.doc_count.toLocaleString()}
								</span>
							</div>
							<div className={s.detailItem}>
								<span className={s.detailLabel}>{t("common.description")}:</span>
								<span className={s.detailValue}>
									{(globalOp && globalOp.description !== undefined
										? globalOp.description
										: details.description) || "-"}
								</span>
							</div>
							<div className={s.detailItemColumn}>
								<span className={s.detailLabelBlock}>
									{t("common.tags")}
								</span>
								{details.tags && details.tags.length > 0 ? (
									<div className={s.badgeList}>
										{details.tags.map((tag) => (
											<Badge
												key={tag}
												variant="blue-subtle"
												size="sm"
											>
												{tag}
											</Badge>
										))}
									</div>
								) : (
									<span className={s.noData}>{t("operationDetails.noTags")}</span>
								)}
							</div>
						</div>
					</div>

					{/* Permissions Section */}
					<div className={s.section}>
						<Stack
							ai="center"
							jc="space-between"
							className={s.sectionTitle}
						>
							<span>{t("common.permissions")}</span>
							<Button
								icon="PenLine"
								variant="secondary"
								className={s.iconButtonTight}
								onClick={() =>
									spawnBanner(
										<OperationPermissions.Banner
											operationId={operationId}
											granted_user_ids={details.granted_user_ids || []}
											granted_user_group_ids={
												details.granted_user_group_ids || []
											}
											onSuccess={() => {
												setLoading(true);
												Info.operation_get_by_id(operationId)
													.then(setDetails)
													.finally(() => setLoading(false));
											}}
										/>,
									)
								}
							/>
						</Stack>
						<div className={s.detailsList}>
							<div className={s.detailItemColumn}>
								<span className={s.detailLabelBlock}>
									{t("common.users")}:
								</span>
								{details.granted_user_ids &&
								details.granted_user_ids.length > 0 ? (
									<div className={s.badgeList}>
										{details.granted_user_ids.map((userId) => (
											<Badge
												key={userId}
												variant="gray-subtle"
												size="sm"
											>
												{userId}
											</Badge>
										))}
									</div>
								) : (
									<span className={s.noData}>{t("operationDetails.noUsers")}</span>
								)}
							</div>

							<div className={cn(s.detailItemColumn, s.detailItemOffset)}>
								<span className={s.detailLabelBlock}>
									{t("common.groups")}:
								</span>
								{details.granted_user_group_ids &&
								details.granted_user_group_ids.length > 0 ? (
									<div className={s.badgeList}>
										{details.granted_user_group_ids.map((groupId) => (
											<Badge
												key={groupId}
												variant="purple-subtle"
												size="sm"
											>
												{groupId}
											</Badge>
										))}
									</div>
								) : (
									<span className={s.noData}>{t("operationDetails.noGroups")}</span>
								)}
							</div>
						</div>
					</div>

					{/* Contexts and Sources Section */}
					{(() => {
						const contexts = Operation.Entity.contexts(app, operationId);
						if (!contexts || contexts.length === 0) return null;
						return (
							<div className={s.section}>
								<div className={s.sectionTitle}>{t("operationDetails.contextsAndSources")}</div>
								<div className={s.detailsListCompact}>
									{contexts.map((context: Context.Type) => {
										const sources = Context.Entity.sources(app, context);
										return (
											<div key={context.id}>
												<ContextHeading
													context={context}
													selectedContexts={new Set<Context.Id>()}
													setContext={() => {}}
													noFiles={sources.length === 0}
													showCheckbox={false}
													onDeleteBack={destroyBanner}
												/>
												{sources.map((source: Source.Type, index: number) => {
													const isLast = index === sources.length - 1;
													return (
														<div
															key={source.id}
															className={cn(
																sFiles.fileWrapper,
																isLast && sFiles.last,
															)}
														>
															<FlatFileComponent
																file={source}
																selectedFiles={new Set<Source.Id>()}
																setFile={() => {}}
																isLast={isLast}
																showFilter={false}
																showCheckbox={false}
																onPreviewBack={() => destroyBanner()}
															/>
														</div>
													);
												})}
											</div>
										);
									})}
								</div>
							</div>
						);
					})()}
				</Stack>
			) : (
				!loading && (
					<div className={cn(s.noData, s.noDataCentered)}>
						{t("operationDetails.loadFailed")}
					</div>
				)
			)}
			<Stack
				jc="flex-end"
				noShrink
				className={s.footer}
			>
				<Button
					variant="tertiary"
					icon="Trash2"
					loading={deleteLoading}
					className={s.deleteButton}
					onClick={openDeleteBanner}
				>
					{t("common.delete")}
				</Button>
			</Stack>
		</Dialog>
	);
}
