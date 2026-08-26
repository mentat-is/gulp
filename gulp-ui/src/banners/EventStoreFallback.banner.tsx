import { Application } from "@/context/Application.context";
import {
	reloadScalableEventPipeline,
	reloadWithLegacyEventPipeline,
	type EventStoreReloadTarget,
	type EventStoreRolloutWritableStorage,
} from "@/event-store/EventStore.rollout";
import type {
	EventStoreFallbackReason,
	EventStoreFallbackRequest,
} from "@/event-store/EventStoreFallback.request";
import { Locale } from "@/locales";
import { Banner as UIBanner } from "@/ui/Banner";
import { Button } from "@/ui/Button";
import { Stack } from "@/ui/Stack";
import { Toggle } from "@/ui/Toggle";
import { useState } from "react";

const REASON_KEYS: Record<EventStoreFallbackReason, string> = {
	quota: "eventStore.fallback.reason.quota",
	storage: "eventStore.fallback.reason.storage",
	"renderer-fatal": "eventStore.fallback.reason.rendererFatal",
	"renderer-unavailable": "eventStore.fallback.reason.rendererUnavailable",
};

export namespace EventStoreFallbackBanner {
	export interface Props {
		request: EventStoreFallbackRequest;
		storage?: EventStoreRolloutWritableStorage;
		reloadTarget?: EventStoreReloadTarget;
	}
}

/**
 * Presents the explicit scalable retry or RAM compatibility decision.
 * @param props Typed failure request and optional browser policy dependencies.
 * @returns Localized compatibility decision banner.
 */
export function EventStoreFallbackBanner({
	request,
	storage,
	reloadTarget,
}: EventStoreFallbackBanner.Props) {
	const { dismissEventStoreFallbackRequest } = Application.use();
	const { language, t } = Locale.use();
	const [compatibilityConfirmed, setCompatibilityConfirmed] =
		useState<boolean>(false);
	const [error, setError] = useState<string | null>(null);

	/** Reloads without changing the scalable EventStore marker. */
	const retryScalable = (): void => {
		if (!reloadScalableEventPipeline(reloadTarget)) {
			setError(t("eventStore.fallback.reloadFailed"));
			return;
		}
		dismissEventStoreFallbackRequest();
	};

	/** Persists and verifies compatibility mode only after explicit confirmation. */
	const useCompatibility = (): void => {
		if (!compatibilityConfirmed) return;
		if (!reloadWithLegacyEventPipeline(storage, reloadTarget)) {
			setError(t("eventStore.fallback.storageFailed"));
			return;
		}
		dismissEventStoreFallbackRequest();
	};

	const estimatedEventCount = new Intl.NumberFormat(language).format(
		request.estimatedEventCount,
	);

	return (
		<UIBanner
			title={t("eventStore.fallback.title")}
			onClose={dismissEventStoreFallbackRequest}
			done={
				<Stack dir="row" gap={8}>
					<Button
						variant="secondary"
						icon="RefreshCw"
						onClick={retryScalable}
					>
						{t("eventStore.fallback.retryScalable")}
					</Button>
					<Button
						variant="destructive"
						icon="Warning"
						disabled={!compatibilityConfirmed}
						onClick={useCompatibility}
					>
						{t("eventStore.fallback.useCompatibility")}
					</Button>
				</Stack>
			}
		>
			<Stack dir="column" ai="unset" gap={8}>
				<p>{t(REASON_KEYS[request.reason])}</p>
				<p>
					{t("eventStore.fallback.estimatedEvents", {
						count: estimatedEventCount,
					})}
				</p>
				<p>{t("eventStore.fallback.ramWarning")}</p>
				{request.message ? <small>{request.message}</small> : null}
				{error ? <p role="alert">{error}</p> : null}
				<Toggle
					option={[
						t("eventStore.fallback.confirmNo"),
						t("eventStore.fallback.confirmYes"),
					]}
					checked={compatibilityConfirmed}
					onCheckedChange={setCompatibilityConfirmed}
				/>
			</Stack>
		</UIBanner>
	);
}
