import s from "./styles/menu.module.css";
import { Application } from "@/context/Application.context";
import { Stack } from "@/ui/Stack";
import { Button } from "@/ui/Button";
import React, { useEffect, useState, useRef } from "react";
import { cn } from "@/ui/utils";
import { Icon } from "@/ui/Icon";
import { useNavigate } from "react-router-dom";
import { Logo } from "./Logo";
import { Locale } from "@/locales";
import { useRequests } from "@/store/request.store";
import { User } from "@/entities/User";

/**
 * Represents a single item rendered inside the Menu component.
 */
export interface MenuItem {
	/** The display label shown when the menu is expanded. */
	label: string;
	/** The icon name from the `@/ui/Icon` library. */
	icon: Icon.Name;
	/** Callback invoked when the item is clicked. */
	action: () => void;
	/**
	 * The category name used to visually group this item under a section header.
	 * Items sharing the same category are rendered together under one header.
	 */
	category: string;
}

/**
 * A single extension plugin entry supplied to the Menu component.
 * Keeps the rendered node and its display title as separate, typed fields
 * to avoid passing `title` through props that don't declare it.
 */
export interface PluginNode {
	/** The rendered Extension.Component node. */
	node: React.ReactNode;
	/** The resolved display title shown in the label overlay when expanded. */
	title: string;
}

export namespace Menu {
	export interface Props {
		/** Items rendered at the top scroll area, grouped by their `category` field. */
		topItems: MenuItem[];
		/** Items rendered in the pinned bottom area, grouped by their `category` field. */
		bottomItems: MenuItem[];
		/**
		 * Extension plugin entries supplied by the consumer.
		 * Menu wraps each node with the expand/collapse-aware `pluginWrapper` styling
		 * and renders the `title` as a label overlay when expanded.
		 */
		pluginNodes?: PluginNode[];
	}
}

/**
 * Internal union representing either a MenuItem or a PluginNode.
 */
type UnifiedMenuEntry =
	| {
			type: "item";
			data: MenuItem;
	  }
	| {
			type: "plugin";
			data: PluginNode;
			index: number;
	  };

/**
 * Groups an array of `MenuItem` objects by their `category` field,
 * preserving the insertion order of categories.
 *
 * @param items - The flat list of menu items to group.
 * @returns An ordered array of `[categoryName, items[]]` tuples.
 */
function groupByCategory(items: MenuItem[]): [string, MenuItem[]][] {
	const map = new Map<string, MenuItem[]>();
	for (const item of items) {
		if (!map.has(item.category)) {
			map.set(item.category, []);
		}
		map.get(item.category)!.push(item);
	}
	return Array.from(map.entries());
}

/**
 * Groups an array of MenuItem objects and an optional list of PluginNode objects
 * by category, merging any plugin nodes under the "Plugins" category.
 *
 * @param items - The list of standard top menu items.
 * @param pluginNodes - The list of plugin nodes, which default to the "Plugins" category.
 * @returns An array of tuples mapping category names to their corresponding unified entries.
 */
function groupTopItemsWithPlugins(
	items: MenuItem[],
	pluginCategory: string,
	pluginNodes?: PluginNode[],
): [string, UnifiedMenuEntry[]][] {
	const map = new Map<string, UnifiedMenuEntry[]>();

	// First, group standard top menu items under their designated categories
	for (const item of items) {
		if (!map.has(item.category)) {
			map.set(item.category, []);
		}
		map.get(item.category)!.push({ type: "item", data: item });
	}

	// Next, append plugin nodes under the "Plugins" category
	if (pluginNodes && pluginNodes.length > 0) {
		const categoryName = pluginCategory;
		if (!map.has(categoryName)) {
			map.set(categoryName, []);
		}
		pluginNodes.forEach((plugin, index) => {
			map.get(categoryName)!.push({
				type: "plugin",
				data: plugin,
				index,
			});
		});
	}

	return Array.from(map.entries());
}

/**
 * Resolves the authenticated user's display label for the account area.
 *
 * @param user - Current authenticated user from application state.
 * @returns A trimmed display name, the user ID fallback, or null when logged out.
 */
function getAuthenticatedUserDisplayName(
	user: User.Type | null,
): string | null {
	if (!user) return null;

	const trimmedName = user.name.trim();
	return trimmedName.length > 0 ? trimmedName : user.id;
}

/**
 * Generic, data-driven side-navigation menu component.
 *
 * Accepts two separate item arrays (`topItems` and `bottomItems`), groups each
 * array by its `category` field, and renders every group as a labeled section.
 * The expand/collapse animation, extension plugin rendering, and request-badge
 * display are handled internally; all business actions are supplied via props.
 *
 * @param props - Component props containing `topItems` and `bottomItems`.
 */
export function Menu({ topItems, bottomItems, pluginNodes }: Menu.Props) {
	const { app, Info } = Application.use();
	const { t } = Locale.use();
	const requests = useRequests();
	const navigate = useNavigate();
	const [isOpen, setIsOpen] = useState(false);
	const menuRef = useRef<HTMLDivElement>(null);

	/** Closes the menu when the user clicks outside of it. */
	useEffect(() => {
		const handleClickOutside = (event: MouseEvent) => {
			if (
				isOpen &&
				menuRef.current &&
				!menuRef.current.contains(event.target as Node)
			) {
				setIsOpen(false);
			}
		};

		document.addEventListener("mousedown", handleClickOutside);
		return () => {
			document.removeEventListener("mousedown", handleClickOutside);
		};
	}, [isOpen]);

	/** Fetches the initial request list on mount. */
	useEffect(() => {
		Info.request_list();
	}, []);

	/**
	 * Invokes the item's action callback and then collapses the menu.
	 *
	 * @param fn - The action callback to invoke, or `undefined` to skip.
	 */
	const handleAction = (fn?: () => void) => {
		if (fn) fn();
		setIsOpen(false);
	};

	/**
	 * Internal button component that applies the correct collapsed/expanded styling.
	 *
	 * @param props - Title, icon, onClick, optional children, and className overrides.
	 */
	const ActionButton = ({
		title,
		icon,
		onClick,
		children,
		className,
	}: {
		title: string;
		icon: Icon.Name;
		onClick?: () => void;
		children?: React.ReactNode;
		className?: string;
	}) => (
		<Button
			variant="tertiary"
			title={title}
			icon={icon}
			size="md"
			className={cn(className, s.actionBtn, isOpen && s.actionBtnExpanded)}
			onClick={() => handleAction(onClick)}
		>
			{isOpen && <span className={s.btnLabel}>{title}</span>}
			{children}
		</Button>
	);

	/**
	 * Renders a list of unified menu entry arrays grouped into labeled category sections.
	 * Handles both standard items and wrapped plugin components.
	 *
	 * @param groups - Ordered `[categoryName, entries[]]` tuples produced by `groupTopItemsWithPlugins`.
	 */
	const renderUnifiedGroups = (groups: [string, UnifiedMenuEntry[]][]) =>
		groups.map(([category, entries]) => (
			<Stack
				key={category}
				dir="column"
				gap={4}
				className={s.section}
			>
				{isOpen && <div className={s.sectionTitle}>{category}</div>}
				{entries.map((entry) => {
					if (entry.type === "item") {
						return (
							<ActionButton
								key={entry.data.label}
								title={entry.data.label}
								icon={entry.data.icon}
								onClick={entry.data.action}
							/>
						);
					} else {
						return (
							<div
								key={`plugin-${entry.index}`}
								onClickCapture={() => setIsOpen(false)}
								className={cn(
									s.pluginWrapper,
									isOpen && s.pluginWrapperExpanded,
								)}
							>
								{entry.data.node}
								{isOpen && (
									<span className={s.pluginLabelOverlay}>
										{entry.data.title}
									</span>
								)}
							</div>
						);
					}
				})}
			</Stack>
		));

	const topGroups = groupTopItemsWithPlugins(
		topItems,
		t("common.plugins"),
		pluginNodes,
	);
	const bottomGroups = groupByCategory(bottomItems);

	/** Count of active (pending/ongoing) requests for the badge indicator. */
	const activeRequestCount = requests.filter(
		(r) => r.status === "pending" || r.status === "ongoing",
	).length;
	const authenticatedUserDisplayName = getAuthenticatedUserDisplayName(
		app.general.user,
	);

	return (
		<div
			ref={menuRef}
			className={cn(s.menuWrapper, isOpen && s.menuWrapperExpanded)}
		>
			<Stack
				className={cn(s.menu, isOpen && s.expanded)}
				dir="column"
				ai={isOpen ? "stretch" : "flex-start"}
				gap={12}
			>
				{isOpen && (
					<Stack
						dir="row"
						jc="space-between"
						className={s.menuHeader}
					>
						<Button
							variant="tertiary"
							title={t("menu.close")}
							icon="SidebarClose"
							size="md"
							onClick={() => setIsOpen(!isOpen)}
						/>
						<Logo
							width={45}
							height={45}
							className={s.gulpLogo}
							onClick={() => {
								navigate("/");
								setIsOpen(false);
							}}
						/>
					</Stack>
				)}
				{!isOpen && (
					<Button
						variant="tertiary"
						title={t("menu.expand")}
						icon="MenuAlt"
						size="md"
						className={cn(s.actionBtn, isOpen && s.actionBtnExpanded)}
						onClick={() => setIsOpen(!isOpen)}
					></Button>
				)}

				<Stack
					ai={isOpen ? "stretch" : "center"}
					jc="flex-start"
					dir="column"
					gap={8}
					className={s.scroll}
				>
					{renderUnifiedGroups(topGroups)}
				</Stack>

				<Stack
					flex
					className={s.spacer}
				/>

				<Stack
					dir="column"
					gap={8}
					ai={isOpen ? "stretch" : "center"}
					className={s.bottomArea}
				>
					{bottomGroups.map(([category, items]) => (
						<React.Fragment key={category}>
							{items.map((item) => {
								/** Special-case: the "Requests" item receives an activity badge. */
								const isRequestsItem = item.icon === "Activity";
								const isLogoutItem = item.icon === "LogOut";
								return (
									<React.Fragment key={item.label}>
										{isOpen && isLogoutItem && authenticatedUserDisplayName && (
											<div
												className={s.userIdentity}
												title={t("auth.loggedInAs", {
													username: authenticatedUserDisplayName,
												})}
											>
												{t("auth.loggedInAs", {
													username: authenticatedUserDisplayName,
												})}
											</div>
										)}
										<ActionButton
											title={item.label}
											icon={item.icon}
											onClick={item.action}
											className={isRequestsItem ? s.requests : undefined}
										>
											{isRequestsItem && (
												<span
													className={cn(
														s.requestsBadge,
														isOpen && s.requestsBadgeExpanded,
													)}
												>
													{activeRequestCount}
												</span>
											)}
										</ActionButton>
									</React.Fragment>
								);
							})}
						</React.Fragment>
					))}
				</Stack>
			</Stack>
		</div>
	);
}
