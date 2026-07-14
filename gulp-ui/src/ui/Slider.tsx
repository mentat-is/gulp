import React from 'react'
import { Root, Track, Thumb, Range } from '@radix-ui/react-slider'
import styles from './styles/Slider.module.css'
import { cn } from '@/ui/utils'

const Slider = React.forwardRef<
  React.ElementRef<typeof Root>,
  React.ComponentPropsWithoutRef<typeof Root>
>(({ className, ...props }, ref) => (
  <Root ref={ref} className={cn(styles.root, className)} {...props}>
    <Track className={styles.track}>
      <Range className={styles.range} />
    </Track>
    <Thumb className={styles.thumb} />
  </Root>
))
Slider.displayName = Root.displayName

export { Slider }
