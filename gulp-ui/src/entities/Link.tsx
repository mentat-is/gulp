import { Doc } from "./Doc";
import { Operation } from "./Operation";
type UUID = string;
import { App } from "./App";
import { Source } from "./Source";
import { Default } from "@/dto/Dataset";
import { Glyph } from "./Glyph";
import { DataStore } from "@/store/DataStore";
import { Banner as UIBanner } from "@/ui/Banner";
import { Application } from "@/context/Application.context";
import { Button } from "@/ui/Button";
import { Toggle } from "@/ui/Toggle";
import { useState } from "react";
import { toast } from "sonner";
import { Internal } from "./addon/Internal";
import { Locale } from "@/locales";
import { EVENT_MAIN_CACHE_SIZE } from "@/event-store";

export namespace Link {
	export const name = "Link";
	const _ = Symbol(Link.name);
	export type Id = UUID & {
		readonly [_]: unique symbol;
	};

	export interface Type {
		id: Link.Id;
		type: "link";
		owner_user_id: string;
		description: string;
		operation_id: Operation.Id;
		tags: string[];
		color: string;
		doc_id_from: Doc.Id;
		doc_ids: Doc.Id[];
		glyph_id: Glyph.Id;
		[key: string]: any;
	}

	/** Existing GulpCollabFilter fields used by paginated link-list requests. */
	export interface ListFilter {
		ids?: Link.Id[];
		limit?: number;
		offset?: number;
	}

	export class Entity {
		public static icon = Internal.IconExtractor.activate<Link.Type | null>(
			Default.Icon.LINK,
		);

		public static id = (_app: App.Type, id: Link.Id): Link.Type =>
			DataStore.links.find((link) => link.id === id)!;

		/**
		 * Resolves links whose destination documents all belong to selected sources.
		 * IndexedDB reads remain bounded even when an overlay contains many endpoints.
		 * @param app Current application state used to resolve selected sources.
		 * @returns Links entirely contained in the current source selection.
		 */
		public static selected = async (app: App.Type): Promise<Link.Type[]> => {
			const selectedSourceIds = new Set(
				Source.Entity.selected(app).map((source) => source.id),
			);
			const selectedLinks: Link.Type[] = [];

			for (const link of DataStore.links) {
				let isSelected = true;
				for (
					let index = 0;
					index < link.doc_ids.length;
					index += EVENT_MAIN_CACHE_SIZE
				) {
					const documentIds = link.doc_ids.slice(
						index,
						index + EVENT_MAIN_CACHE_SIZE,
					);
					const documents = await DataStore.Events.getByIds(documentIds);
					if (
						documents.length !== documentIds.length ||
						documents.some(
							(document) =>
								!selectedSourceIds.has(document["gulp.source_id"]),
						)
					) {
						isSelected = false;
						break;
					}
				}
				if (isSelected) selectedLinks.push(link);
			}

			return selectedLinks;
		};

		/**
		 * Calculates the average timestamp of the currently persisted link targets.
		 * @param _app Retained application argument for the existing entity API.
		 * @param link Link whose destination timestamps should be averaged.
		 * @returns Average timestamp, or zero when no destination can be resolved.
		 */
		public static timestamp = async (
			_app: App.Type,
			link: Link.Type,
		): Promise<number> => {
			if (link.doc_ids.length === 0) {
				return 0;
			}

			let sum = 0;
			let count = 0;
			for (
				let index = 0;
				index < link.doc_ids.length;
				index += EVENT_MAIN_CACHE_SIZE
			) {
				const documents = await DataStore.Events.getByIds(
					link.doc_ids.slice(index, index + EVENT_MAIN_CACHE_SIZE),
				);
				for (const document of documents) {
					sum += document.gulp_timestamp;
					count++;
				}
			}

			return count === 0 ? 0 : sum / count;
		};
	}

	export namespace Delete {
		export namespace Banner {
			export interface Props extends UIBanner.Props {
				link: Link.Type;
				/** Called after the link is removed from the canonical DataStore. */
				onDeleted?: () => void;
			}
		}
		export function Banner({
			link,
			onDeleted,
			...props
		}: Link.Delete.Banner.Props) {
			const { Info, destroyBanner } = Application.use();
			const { t } = Locale.use();
			const [loading, setLoading] = useState<boolean>(false);
			const [isSubmited, setIsSubmited] = useState<boolean>(false);

			const DeleteButton = () => (
				<Button
					loading={loading}
					icon="Trash2"
					variant="glass"
					onClick={deleteLink}
					disabled={!isSubmited}
				/>
			);

			const deleteLink = async () => {
				setLoading(true);
				await Info.link_delete(link);
				setLoading(false);
				onDeleted?.();
				if (props.back) {
					props.back();
				} else {
					destroyBanner();
				}
				toast(t("link.deleted", { name: link.name }));
			};

			return (
				<UIBanner
					title={t("link.deleteTitle")}
					done={<DeleteButton />}
					{...props}
				>
					<p>
						{t("link.deleteConfirm")} <code>{link.name}</code>
					</p>
					<Toggle
						option={[t("common.noDontDelete"), t("common.yesImSure")]}
						checked={isSubmited}
						onCheckedChange={setIsSubmited}
					/>
				</UIBanner>
			);
		}
	}
}
