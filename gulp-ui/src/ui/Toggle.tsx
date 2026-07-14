import { cn } from '@/ui/utils'
import { Switch, SwitchProps } from './Switch'
import s from './styles/Toggle.module.css'
import { Stack } from './Stack'

interface ToggleProps extends SwitchProps {
  option: [string, string]
}

export function Toggle({ option, className, ...props }: ToggleProps) {
  return (
    <Stack ai="center" flex className={cn(s.toggle, className)}>
      <p className={cn(!props.checked && s.selected)}>{option[0]}</p>
      <Switch {...props} />
      <p className={cn(props.checked && s.selected)}>{option[1]}</p>
    </Stack>
  )
}
