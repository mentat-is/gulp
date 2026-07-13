import * as Dialog from "@radix-ui/react-dialog";
import { KeyboardEvent, MouseEvent, useState } from "react";
import { Button } from "@/ui/Button";
import { Label } from "@/ui/Label";
import { Stack } from "@/ui/Stack";
import { cn } from "@/ui/utils";
import { SavedPluginMapping } from "./mappingPersistence";
import s from "../styles/AdvancedPluginParams.module.css";

interface LastMappingsPanelProps {
	open: boolean;
	setOpen: (open: boolean) => void;
	savedMappings: SavedPluginMapping[];
	loading: boolean;
	onLoad: (mapping: SavedPluginMapping) => void;
	onDelete: (mapping: SavedPluginMapping) => void | Promise<void>;
}

/**
 * Formats a persisted mapping timestamp for compact display.
 *
 * @param value - ISO date string saved with the mapping record.
 * @returns A locale-aware date string, or an empty value when parsing fails.
 */
function formatSavedMappingDate(value: string): string {
	const date = new Date(value);

	if (Number.isNaN(date.getTime())) {
		return "";
	}

	return date.toLocaleString();
}

/**
 * LastMappingsPanel renders saved custom mappings for the selected plugin.
 */
export function LastMappingsPanel({
	open,
	setOpen,
	savedMappings,
	loading,
	onLoad,
	onDelete,
}: LastMappingsPanelProps) {
	const [previewMapping, setPreviewMapping] =
		useState<SavedPluginMapping | null>(null);

	/**
	 * Applies a saved mapping when the user activates a list row with the keyboard.
	 *
	 * @param event - Keyboard event fired from the mapping row.
	 * @param savedMapping - Saved mapping represented by the focused row.
	 * @returns void
	 */
	const handleMappingRowKeyDown = (
		event: KeyboardEvent<HTMLDivElement>,
		savedMapping: SavedPluginMapping,
	) => {
		if (event.key !== "Enter" && event.key !== " ") {
			return;
		}

		event.preventDefault();
		onLoad(savedMapping);
	};

	/**
	 * Opens the JSON preview for a saved mapping.
	 *
	 * @param event - Mouse event from the preview action button.
	 * @param savedMapping - Mapping whose payload should be previewed.
	 * @returns void
	 */
	const handlePreview = (
		event: MouseEvent<HTMLButtonElement>,
		savedMapping: SavedPluginMapping,
	) => {
		event.stopPropagation();
		setPreviewMapping(savedMapping);
	};

	/**
	 * Deletes the saved mapping represented by a list row.
	 *
	 * @param event - Mouse event from the delete action button.
	 * @param savedMapping - Mapping to remove from persistence.
	 * @returns A promise that resolves after the delete callback is handled.
	 */
	const handleDelete = async (
		event: MouseEvent<HTMLButtonElement>,
		savedMapping: SavedPluginMapping,
	) => {
		event.stopPropagation();
		await onDelete(savedMapping);
	};

	/**
	 * Gets the secondary summary shown below the mapping identifier.
	 *
	 * @param savedMapping - Saved mapping record to summarize.
	 * @returns A compact description for the custom list row.
	 */
	const getSavedMappingSummary = (savedMapping: SavedPluginMapping): string => {
		return (
			savedMapping.mapping.description ||
			savedMapping.mapping.agent_type ||
			"Custom mapping"
		);
	};

	return (
		<>
			<Dialog.Root
				open={open}
				onOpenChange={setOpen}
			>
				<Dialog.Portal>
					<Dialog.Overlay className={s.overlayL2} />
					<Dialog.Content
						aria-describedby={undefined}
						className={cn(s.contentL2Small, s.lastMappingsContent)}
					>
						<Stack
							dir="column"
							gap={16}
							ai="stretch"
						>
							<Dialog.Title className={cn(s.titleBase, s.titleM)}>
								Last Mappings
							</Dialog.Title>

							<Stack
								dir="column"
								gap={8}
								ai="stretch"
								className={s.savedMappingsList}
							>
								{loading && (
									<Label
										value="Loading mappings..."
										className={s.savedMappingsEmpty}
									/>
								)}

								{!loading && savedMappings.length === 0 && (
									<Label
										value="No saved mappings"
										className={s.savedMappingsEmpty}
									/>
								)}

								{!loading &&
									savedMappings.map((savedMapping) => (
										<div
											key={savedMapping.mapping_id}
											role="button"
											tabIndex={0}
											className={s.savedMappingItem}
											onClick={() => onLoad(savedMapping)}
											onKeyDown={(event) =>
												handleMappingRowKeyDown(event, savedMapping)
											}
										>
											<Stack
												dir="column"
												gap={4}
												ai="stretch"
												className={s.savedMappingDetails}
											>
												<span className={s.savedMappingTitle}>
													{savedMapping.mapping_id}
												</span>
												<span className={s.savedMappingMeta}>
													{getSavedMappingSummary(savedMapping)}
												</span>
												<span className={s.savedMappingDate}>
													{formatSavedMappingDate(savedMapping.updated_at)}
												</span>
											</Stack>

											<Stack
												dir="row"
												gap={4}
												ai="center"
												className={s.savedMappingActions}
											>
												<Button
													variant="glass"
													icon="MagnifyingGlass"
													shape="icon"
													aria-label={`Preview ${savedMapping.mapping_id}`}
													onClick={(event) =>
														handlePreview(event, savedMapping)
													}
												/>
												<Button
													variant="tertiary"
													icon="Trash"
													shape="icon"
													aria-label={`Delete ${savedMapping.mapping_id}`}
													onClick={(event) => handleDelete(event, savedMapping)}
												/>
											</Stack>
										</div>
									))}
							</Stack>

							<Stack
								dir="row"
								gap={8}
								jc="flex-end"
							>
								<Button
									variant="secondary"
									onClick={() => setOpen(false)}
								>
									Cancel
								</Button>
							</Stack>
						</Stack>
					</Dialog.Content>
				</Dialog.Portal>
			</Dialog.Root>

			<Dialog.Root
				open={!!previewMapping}
				onOpenChange={(isOpen) => !isOpen && setPreviewMapping(null)}
			>
				<Dialog.Portal>
					<Dialog.Overlay className={s.overlayL3} />
					<Dialog.Content
						aria-describedby={undefined}
						className={cn(s.contentL3, s.mappingPreviewContent)}
					>
						<Stack
							dir="column"
							gap={16}
							ai="stretch"
						>
							<Dialog.Title className={cn(s.titleBase, s.titleM)}>
								{previewMapping
									? `Preview ${previewMapping.mapping_id}`
									: "Preview Mapping"}
							</Dialog.Title>
							<pre className={s.mappingPreviewCode}>
								{previewMapping
									? JSON.stringify(previewMapping.mapping, null, 2)
									: ""}
							</pre>
							<Stack
								dir="row"
								gap={8}
								jc="flex-end"
							>
								<Button
									variant="secondary"
									onClick={() => setPreviewMapping(null)}
								>
									Close
								</Button>
							</Stack>
						</Stack>
					</Dialog.Content>
				</Dialog.Portal>
			</Dialog.Root>
		</>
	);
}
