import { Toaster as Sonner } from 'sonner'
import s from './styles/Toaster.module.css'
import { useTheme } from 'next-themes'

type ToasterProps = React.ComponentProps<typeof Sonner>

type RGB = { r: number; g: number; b: number }

const parseCssColorToRgb = (value: string): RGB | null => {
  const v = value.trim();

  if (v.startsWith('#')) {
    const hex = v.replace('#', '');
    if (hex.length === 3) {
      const r = parseInt(hex[0] + hex[0], 16);
      const g = parseInt(hex[1] + hex[1], 16);
      const b = parseInt(hex[2] + hex[2], 16);
      return Number.isNaN(r + g + b) ? null : { r, g, b };
    }

    if (hex.length === 6) {
      const r = parseInt(hex.slice(0, 2), 16);
      const g = parseInt(hex.slice(2, 4), 16);
      const b = parseInt(hex.slice(4, 6), 16);
      return Number.isNaN(r + g + b) ? null : { r, g, b };
    }
  }

  const rgbMatch = v.match(/^rgba?\(([^)]+)\)$/i);
  if (rgbMatch) {
    const channels = rgbMatch[1].split(',').map((part) => Number(part.trim()));
    if (channels.length >= 3 && !channels.slice(0, 3).some((channel) => Number.isNaN(channel))) {
      return { r: channels[0], g: channels[1], b: channels[2] };
    }
  }

  return null;
}

const getSonnerThemeFromCss = (): 'light' | 'dark' => {
  if (typeof window === 'undefined') {
    return 'dark';
  }

  const background = getComputedStyle(document.documentElement)
    .getPropertyValue('--background-200')
    .trim();

  const rgb = parseCssColorToRgb(background);
  if (!rgb) {
    return 'dark';
  }

  const luminance = (0.299 * rgb.r + 0.587 * rgb.g + 0.114 * rgb.b) / 255;
  return luminance > 0.55 ? 'light' : 'dark';
}

const Toaster = ({ ...props }: ToasterProps) => {
  const { theme } = useTheme();

  return (
    <Sonner
      key={theme}
      theme={getSonnerThemeFromCss()}
      className={s.toaster}
      toastOptions={{
        classNames: {
          toast: s.toast,
          description: s.description,
          actionButton: s.actionButton,
          cancelButton: s.cancelButton,
        },
      }}
      {...props}
    />
  )
}

export { Toaster }
