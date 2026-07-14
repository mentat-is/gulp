import { Icon, iconMap } from './Icon'

export const icons = iconMap

export const ICON_VARIANT_COLORS = {
    default: 'currentColor',
    white: '#e8e8e8',
    dimmed: '#a1a1a1',
    black: '#0d0d0d',
} as const

export type IconName = Icon.Name

export type IconComponent = (typeof icons)[IconName]

export type IconProps = Icon.Props

export type IconSize = Icon.Size

export const getIcon = (name: IconName): IconComponent => icons[name]
