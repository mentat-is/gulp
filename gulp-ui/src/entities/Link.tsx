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

	export class Entity {
		public static icon = Internal.IconExtractor.activate<Link.Type | null>(
			Default.Icon.LINK,
		);

		public static id = (_app: App.Type, id: Link.Id): Link.Type =>
			DataStore.links.find((link) => link.id === id)!;

		public static selected = (app: App.Type) =>
			DataStore.links.filter((link) =>
				link.doc_ids.every(
					(id) =>
						Source.Entity.id(app, Doc.Entity.id(app, id)?.["gulp.source_id"])
							?.selected,
				),
			);

		public static timestamp = (app: App.Type, link: Link.Type): number => {
			if (link.doc_ids.length === 0) {
				return 0;
			}

			let sum = 0;

			link.doc_ids.forEach(
				(d) => (sum += Doc.Entity.id(app, d).gulp_timestamp),
			);

			return sum / link.doc_ids.length;
		};
	}

	export namespace Delete {
		export namespace Banner {
			export interface Props extends UIBanner.Props {
				link: Link.Type;
			}
		}
		export function Banner({ link, ...props }: Link.Delete.Banner.Props) {
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
