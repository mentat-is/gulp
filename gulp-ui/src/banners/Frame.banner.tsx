import { useCallback, useEffect, useRef, useState } from "react";
import { format } from "date-fns";
import { Banner as UIBanner } from "../ui/Banner";
import { Application } from "../context/Application.context";
import type { MinMax } from "@/class/Info";
import { Context } from "@/entities/Context";
import { Source } from "@/entities/Source";
import { Logger } from "@/dto/Logger.class";
import { Locale } from "@/locales";
import { Button } from "@/ui/Button";
import { Input } from "@/ui/Input";
import { Stack } from "@/ui/Stack";
import { Toggle } from "@/ui/Toggle";
import { useRequestLoadings } from "@/store/request.store";
import s from "./styles/LimitsBanner.module.css";

type FrameBoundary = keyof MinMax;

interface DateSelectionProps {
	type: FrameBoundary;
	value: number;
	valid: boolean;
	manual: boolean;
	label: string;
	placeholder: string;
	onCommit: (type: FrameBoundary, value: string) => void;
}

/**
 * Formats one frame boundary for the active date-entry mode.
 * @param value Millisecond timestamp to display.
 * @param manual Whether the editor is in ISO-string mode.
 * @returns Browser-local or ISO-formatted input value.
 */
function formatSelectionValue(value: number, manual: boolean): string {
	return manual
		? new Date(value).toISOString()
		: format(value, "yyyy-MM-dd'T'HH:mm");
}

/**
 * Edits one frame boundary while retaining the draft until it is committed.
 * Keeping this component at module scope preserves its state when the banner
 * rerenders for request-loading updates.
 * @param props Date-selection configuration and commit callback.
 * @returns A date or ISO-string input for one frame boundary.
 */
function DateSelection({
	type,
	value,
	valid,
	manual,
	label,
	placeholder,
	onCommit,
}: DateSelectionProps) {
	const inputRef = useRef<HTMLInputElement | null>(null);
	const formattedValue = formatSelectionValue(value, manual);
	const [localValue, setLocalValue] = useState(formattedValue);

	useEffect(() => {
		setLocalValue(formattedValue);
	}, [formattedValue]);

	/** Opens the browser's native date picker from the shared input icon. */
	const showPicker = useCallback((): void => {
		inputRef.current?.showPicker?.();
	}, []);

	return (
		<Input
			ref={inputRef}
			label={label}
			type={manual ? "text" : "datetime-local"}
			valid={valid}
			variant="highlighted"
			icon="Calendar"
			onIconClick={manual ? undefined : showPicker}
			value={localValue}
			onChange={(event) => setLocalValue(event.target.value)}
			onBlur={() => onCommit(type, localValue)}
			className={manual ? undefined : s.input}
			placeholder={manual ? placeholder : undefined}
		/>
	);
}

export namespace Frame {
	export namespace Banner {
		export interface Props extends UIBanner.Props {
			frame?: MinMax;
			callback?: (frame: MinMax) => void;
		}
	}

	/**
	 * Selects and persists the visible timeline frame.
	 * @param props Initial frame, optional callback, and shared banner options.
	 * @returns Timeline frame selection banner.
	 */
	export function Banner({
		frame: initFrame,
		callback,
		...props
	}: Frame.Banner.Props) {
		const { Info, destroyBanner, app } = Application.use();
		const { t } = Locale.use();
		const loadings = useRequestLoadings();
		const [frame, setFrame] = useState<MinMax>(
			initFrame ?? Context.Entity.frame(app),
		);
		const [isMinValid, setIsMinValid] = useState(true);
		const [isMaxValid, setIsMaxValid] = useState(true);
		const [manual, setManual] = useState(false);

		/**
		 * Persists the selected frame or returns it to the caller.
		 * @param minimum Optional preset lower boundary.
		 * @returns Nothing after the frame action is dispatched.
		 */
		const save = async (minimum?: number): Promise<void> => {
			const nextFrame = {
				min: minimum ?? frame.min,
				max: frame.max,
			};

			if (callback) {
				callback(nextFrame);
				return;
			}

			Info.setTimelineFrame(nextFrame);
			Info.refetch({
				ids: Source.Entity.selected(app)
					.map((file) => file.id)
					.filter((id) => !loadings.byFileId.has(id)),
			});
			destroyBanner();
		};

		/**
		 * Updates the validity marker for the boundary the user changed.
		 * @param type Frame boundary being validated.
		 * @param nextFrame Frame containing the newly parsed boundary.
		 * @returns Nothing.
		 */
		const validate = useCallback(
			(type: FrameBoundary, nextFrame: MinMax): void => {
				const valid = nextFrame.min < nextFrame.max;
				if (type === "min") {
					setIsMinValid(valid);
				} else {
					setIsMaxValid(valid);
				}
			},
			[],
		);

		/**
		 * Parses and commits one date draft against the latest frame.
		 * @param type Frame boundary being changed.
		 * @param value Local-date or ISO input value.
		 * @returns Nothing.
		 */
		const handleDateChange = useCallback(
			(type: FrameBoundary, value: string): void => {
				try {
					const timestamp = new Date(value).valueOf();
					if (Number.isNaN(timestamp) || timestamp === 0) {
						Logger.error(`Invalid date: ${value}`);
						validate(type, frame);
						return;
					}

					const nextFrame = { ...frame, [type]: timestamp };
					setFrame(nextFrame);
					validate(type, nextFrame);
				} catch {
					validate(type, frame);
				}
			},
			[frame, validate],
		);

		const presets = [
			{
				text: t("frame.lastDay"),
				do: () => save(frame.max - 24 * 60 * 60 * 1000),
			},
			{
				text: t("frame.lastWeek"),
				do: () => save(frame.max - 7 * 24 * 60 * 60 * 1000),
			},
			{
				text: t("frame.lastMonth"),
				do: () => save(frame.max - 30 * 24 * 60 * 60 * 1000),
			},
			{ text: t("frame.fullRange"), do: () => save() },
		];

		return (
			<UIBanner
				className={s.banner}
				title={t("frame.title")}
				done={
					<Button variant="glass" icon="Check" onClick={() => save()} />
				}
				{...props}
			>
				<Toggle
					checked={manual}
					onCheckedChange={setManual}
					option={[
						t("tableView.selectDates"),
						t("tableView.isoString"),
					]}
				/>
				<DateSelection
					type="min"
					value={frame.min}
					valid={isMinValid}
					manual={manual}
					label={t("common.from")}
					placeholder={t("tableView.isoPlaceholder")}
					onCommit={handleDateChange}
				/>
				<DateSelection
					type="max"
					value={frame.max}
					valid={isMaxValid}
					manual={manual}
					label={t("common.to")}
					placeholder={t("tableView.isoPlaceholder")}
					onCommit={handleDateChange}
				/>
				<Stack>
					{presets.map((option) => (
						<Button
							className={s.button}
							variant="secondary"
							onClick={option.do}
							key={option.text}
						>
							{option.text}
						</Button>
					))}
				</Stack>
			</UIBanner>
		);
	}
}
