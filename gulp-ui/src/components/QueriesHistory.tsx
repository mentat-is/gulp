import { memo, useState, useRef } from "react";
import { Popover } from "@/ui/Popover";
import { Button } from "@/ui/Button";
import {
	Tooltip,
	TooltipContent,
	TooltipProvider,
	TooltipTrigger,
} from "@/ui/Tooltip";
import { Separator } from "@/ui/Separator";
import { Query } from "@/entities/Query";
import { Checkbox } from "@/ui/Checkbox";
import { Label } from "@/ui/Label";
import { Stack } from "@/ui/Stack";
import { Icon } from "@/ui/Icon";
import s from "./styles/QueriesHistory.module.css";
import { Locale } from "@/locales";

interface QueriesHistoryProps {
	list: Query.Type[];
	onSelect: (q: Query.Type, applySource: boolean) => void;
}

/**
 * Reusable component displaying a list of previously applied queries in a Popover.
 *
 * @param list - The list of historical Query.Type objects.
 * @param onSelect - Callback when a query is selected for application. Receives the query and whether to apply its source config.
 */
export const QueriesHistory = memo(
	({ list, onSelect }: QueriesHistoryProps) => {
		const [applySource, setApplySource] = useState(true);
		const [isOpen, setIsOpen] = useState(false);
		const { t } = Locale.use();

		if (list.length === 0) return null;

		const handleSelectFilter = (q: Query.Type) => {
			onSelect(q, applySource);
			// Close the popover after selection
			setIsOpen(false);
		};

		return (
			<Popover.Root
				open={isOpen}
				onOpenChange={setIsOpen}
			>
				<Popover.Trigger asChild>
					<Button
						icon="ClockFading"
						variant="secondary"
					>
						{t("queriesHistory.lastFilters")}
					</Button>
				</Popover.Trigger>
				<Popover.Content className={s.lastFilters}>
					<div className={s.lastFiltersList}>
						<TooltipProvider>
							<Tooltip>
								<TooltipTrigger asChild>
									<Stack
										dir="row"
										ai="center"
										gap={8}
										className={s.applySourceOption}
									>
										<Checkbox
											id="apply-source-checkbox"
											checked={applySource}
											onCheckedChange={(checked) => setApplySource(!!checked)}
										/>
										<Label
											htmlFor="apply-source-checkbox"
											value={t("queriesHistory.applySources")}
											cursor="pointer"
										/>
										<Icon
											name="Info"
											style={{ width: 14, height: 14, opacity: 0.6 }}
										/>
									</Stack>
								</TooltipTrigger>
								<TooltipContent className={s.applySourceTooltip}>
									{t("queriesHistory.applySourcesTooltip")}
								</TooltipContent>
							</Tooltip>
						</TooltipProvider>
						<Separator className={s.applySourceSeparator} />
						<div className={s.filterListContainer}>
							{list.map((q, i) => (
								<TooltipProvider key={i}>
									<Tooltip>
										<TooltipTrigger asChild>
											<button
												className={s.filterListItem}
												onClick={() => handleSelectFilter(q)}
											>
												<div className={s.filterItemContent}>
													<div className={s.filterItemText}>{q.string}</div>
												</div>
												<Icon
													name="ChevronRight"
													className={s.filterItemIcon}
												/>
											</button>
										</TooltipTrigger>
										<TooltipContent className={s.tooltip}>
											<pre
												style={{ textAlign: "left", whiteSpace: "pre-wrap" }}
											>
												{q.string}
											</pre>
											{q.filters && q.filters.length > 0 && (
												<pre
													style={{
														textAlign: "left",
														whiteSpace: "pre-wrap",
														marginTop: 4,
													}}
												>
													{JSON.stringify(q.filters, null, 2)}
												</pre>
											)}
										</TooltipContent>
									</Tooltip>
								</TooltipProvider>
							))}
						</div>
					</div>
				</Popover.Content>
			</Popover.Root>
		);
	},
);
