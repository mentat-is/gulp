import { ComponentProps } from 'react'
import * as LucideIcons from 'lucide-react'
import type { LucideIcon } from 'lucide-react'

const VARIANT_COLORS = {
    default: 'currentColor',
    white: '#e8e8e8',
    dimmed: '#a1a1a1',
    black: '#0d0d0d',
} as const

const DEFAULT_ICON_SIZE = 16

const isLucideIcon = (component: unknown): component is LucideIcon =>
    typeof component === 'object' && component !== null && '$$typeof' in component

export const iconMap = Object.fromEntries(
    Object.entries(LucideIcons).filter(([name, component]) =>
        /^[A-Z]/.test(name) && !name.endsWith('Icon') && isLucideIcon(component)
    )
) as Record<string, LucideIcon>

Object.assign(iconMap, {
    AcronymJson: LucideIcons.FileJson,
    AcronymMarkdown: LucideIcons.FileText,
    ChevronRightSmall: LucideIcons.ChevronRight,
    ClockRewind: LucideIcons.History,
    CodeBracket: LucideIcons.Brackets,
    FaceSad: LucideIcons.Frown,
    FaceUnhappy: LucideIcons.Frown,
    Gps: LucideIcons.LocateFixed,
    Information: LucideIcons.Info,
    LayoutShift: LucideIcons.PanelsTopLeft,
    LogoGoogle: LucideIcons.Chrome,
    LogoMicrosoft: LucideIcons.Grid2x2,
    Logout: LucideIcons.LogOut,
    MagnifyingGlass: LucideIcons.Search,
    MagnifyingGlassSmall: LucideIcons.Search,
    MenuAlt: LucideIcons.Menu,
    PencilEdit: LucideIcons.PencilLine,
    PreviewDocument: LucideIcons.FileSearch,
    PreviewEye: LucideIcons.Eye,
    PrismColor: LucideIcons.Palette,
    RefreshClockwise: LucideIcons.RefreshCw,
    Status: LucideIcons.CircleDot,
    Stop: LucideIcons.CircleStop,
    TextTitle: LucideIcons.Heading,
    Warning: LucideIcons.TriangleAlert,
    ai_filter_request: LucideIcons.WandSparkles,
    'glyph-search': LucideIcons.Search,
    google: LucideIcons.Chrome,
    microsoft: LucideIcons.Grid2x2,
} satisfies Record<string, LucideIcon>)

/**
 * Renders a named SVG icon while preserving the legacy default icon size.
 *
 * @param props - Icon name, color variant, explicit color, size, and SVG attributes.
 * @returns The matching icon component, or a fallback help icon when the name is unknown.
 */
export function Icon({
    name,
    variant = 'default',
    color,
    size = DEFAULT_ICON_SIZE,
    ...props
}: Icon.Props) {
    const resolvedColor = color ?? VARIANT_COLORS[variant]
    const LucideIcon = iconMap[name] ?? LucideIcons.CircleHelp

    return <LucideIcon color={resolvedColor} size={size} {...props} />
}

export namespace Icon {
    export type Name = string

    export type Size = number

    export interface Props extends Omit<ComponentProps<LucideIcon>, 'name' | 'variant' | 'color' | 'size'> {
        name: Name
        color?: string
        size?: Size
        variant?: keyof typeof VARIANT_COLORS
    }

    export const icons = iconMap
}
