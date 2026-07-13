import { ReactNode, useState } from "react";
import { Application } from "@/context/Application.context";
import { Banner as UIBanner } from "@/ui/Banner";
import { Button } from "@/ui/Button";
import { Toggle } from "@/ui/Toggle";
import { Locale } from "@/locales";

export namespace DeleteConfirmationBanner {
	export interface Props extends UIBanner.Props {
		/** Content describing the delete action the user is confirming. */
		children: ReactNode;
		/** Callback executed after the user confirms the delete action. */
		onConfirm: () => void | boolean | Promise<void | boolean>;
	}
}

/**
 * Renders a shared delete confirmation banner with the standard confirmation toggle.
 *
 * @param props - Banner content and confirmation callback.
 * @returns A localized confirmation banner for destructive delete actions.
 */
export function DeleteConfirmationBanner({
	children,
	onConfirm,
	...props
}: DeleteConfirmationBanner.Props) {
	const { destroyBanner } = Application.use();
	const { t } = Locale.use();
	const [loading, setLoading] = useState<boolean>(false);
	const [isSubmitted, setIsSubmitted] = useState<boolean>(false);

	/**
	 * Runs the caller-provided delete callback once the confirmation toggle is set.
	 *
	 * @returns A promise that settles after the delete callback finishes.
	 */
	const handleConfirmDelete = async () => {
		setLoading(true);
		let shouldClose = false;
		try {
			const result = await onConfirm();
			shouldClose = result !== false;
		} finally {
			setLoading(false);
		}

		if (shouldClose) {
			destroyBanner();
		}
	};

	return (
		<UIBanner
			title={t("common.delete")}
			done={
				<Button
					loading={loading}
					icon="Trash2"
					variant="destructive"
					onClick={handleConfirmDelete}
					disabled={!isSubmitted}
				>
					{t("common.delete")}
				</Button>
			}
			{...props}
		>
			{children}
			<Toggle
				option={[t("common.noDontDelete"), t("common.yesImSure")]}
				checked={isSubmitted}
				onCheckedChange={setIsSubmitted}
			/>
		</UIBanner>
	);
}
