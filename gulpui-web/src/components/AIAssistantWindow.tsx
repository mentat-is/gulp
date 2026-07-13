import "@/global.css";
import { AIAssistant } from "@/banners/AIAssistant.banner";
import { Extension } from "@/context/Extension.context";
import s from "./styles/AIAssistantWindow.module.css";

interface AIAssistantWindowProps {
	title: string;
	pluginFilename?: string;
	onClose: () => void;
}

/**
 * AIAssistantWindow component for detached AI Assistant windows.
 * Renders either the built-in assistant or the selected assistant plugin.
 */
export function AIAssistantWindow({
	title,
	pluginFilename,
	onClose,
}: AIAssistantWindowProps) {
	return (
		<div className={s.main}>
			<div className={s.header}>
				<h2>{title}</h2>
			</div>
			<div className={s.content}>
				{pluginFilename ? (
					<Extension.Component name={pluginFilename} />
				) : (
					<AIAssistant.Panel />
				)}
			</div>
		</div>
	);
}
