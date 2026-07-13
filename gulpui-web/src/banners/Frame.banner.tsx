import { useState, useRef, useEffect, useCallback } from 'react'
import { Banner as UIBanner } from '../ui/Banner'
import { Application } from '../context/Application.context'
import { Toggle } from '@/ui/Toggle'
import s from './styles/LimitsBanner.module.css'
import { MinMax } from '@/class/Info'
import { format } from 'date-fns'
import { Logger } from '@/dto/Logger.class'
import { Input } from '@/ui/Input'
import { Button } from '@/ui/Button'
import { Stack } from '@/ui/Stack'
import { Context } from '@/entities/Context'
import { Source } from '@/entities/Source'
import { Locale } from '@/locales'
import { useRequestLoadings } from '@/store/request.store'

export namespace Frame {
  export namespace Banner {
    export interface Props extends UIBanner.Props {
      frame?: MinMax;
      callback?: (frame: MinMax) => void;
    }
  }
  export function Banner({ frame: initFrame, callback, ...props }: Frame.Banner.Props) {
    const { Info, destroyBanner, app } = Application.use()
    const { t } = Locale.use()
    const loadings = useRequestLoadings()
    const [frame, setFrame] = useState<MinMax>(initFrame ?? Context.Entity.frame(app))
    const [isMinValid, setIsMinValid] = useState<boolean>(true)
    const [isMaxValid, setIsMaxValid] = useState<boolean>(true)
    const [manual, setManual] = useState<boolean>(false)

    const map = [
      { text: t('frame.lastDay'), do: () => save(frame.max - 24 * 60 * 60 * 1000) },
      { text: t('frame.lastWeek'), do: () => save(frame.max - 7 * 24 * 60 * 60 * 1000) },
      { text: t('frame.lastMonth'), do: () => save(frame.max - 30 * 24 * 60 * 60 * 1000) },
      { text: t('frame.fullRange'), do: () => save() },
    ]

    const save = async (_min?: number) => {
      const { min, max } = { min: _min ?? frame.min, max: frame.max }

      if (callback) {
        return callback({ min, max });
      }
      Info.setTimelineFrame({ min, max });
      Info.refetch({
        ids: Source.Entity.selected(app).map(file => file.id).filter(id => !loadings.byFileId.has(id))
      });
      destroyBanner()
    }

    const validate = (type: keyof MinMax) => {
      if (type === 'min') {
        setIsMinValid(frame.min < frame.max)
      } else {
        setIsMaxValid(frame.max > frame.min)
      }
    }

    const handleDateChange = useCallback((type: keyof MinMax, value: number | string) => {
      try {
        const timestamp = new Date(value).valueOf()
        if (isNaN(timestamp) || timestamp === 0) {
          Logger.error(`Invalid date: ${value}`)
          validate(type)
          return
        }
        setFrame((prev) => ({ ...prev, [type]: timestamp }))
        validate(type)
      } catch {
        validate(type)
      }
    }, [])

    const InputDateSelection = ({ type }: { type: keyof MinMax }) => {
      const inputRef = useRef<HTMLInputElement | null>(null)
      const [localValue, setLocalValue] = useState(format(frame[type], "yyyy-MM-dd'T'HH:mm"))

      useEffect(() => {
        setLocalValue(format(frame[type], "yyyy-MM-dd'T'HH:mm"))
      }, [frame[type]])

      useEffect(() => {
        const input = inputRef.current
        const icon = input?.parentElement?.querySelector('svg')

        const clickHandler = () => input?.showPicker?.()

        icon?.addEventListener('click', clickHandler)
        return () => icon?.removeEventListener('click', clickHandler)
      }, [])

      return (
        <Input
          ref={inputRef}
          label={type === 'min' ? t('common.from') : t('common.to')}
          type="datetime-local"
          valid={type === 'min' ? isMinValid : isMaxValid}
          variant="highlighted"
          icon="Calendar"
          value={localValue}
          onChange={(e) => setLocalValue(e.target.value)}
          onBlur={() => handleDateChange(type, localValue)}
          className={s.input}
        />
      )
    }

    function TextDateSelection({ type }: { type: keyof MinMax }) {
      return (
        <Input
          type="text"
          label={type === 'min' ? t('common.from') : t('common.to')}
          valid={type === 'min' ? isMinValid : isMaxValid}
          value={new Date(frame[type]).toISOString()}
          icon="Calendar"
          variant="highlighted"
          onChange={(e) => handleDateChange(type, e.target.value)}
          placeholder={t('tableView.isoPlaceholder')}
        />
      )
    }

    function DateSelection({ type }: { type: keyof MinMax }) {
      return manual ? (
        <TextDateSelection type={type} />
      ) : (
        <InputDateSelection type={type} />
      )
    }

    const Done = () => (
      <Button variant="glass" icon="Check" onClick={() => save()} />
    )

    return (
      <UIBanner className={s.banner} title={t('frame.title')} done={<Done />} {...props}>
        <Toggle
          checked={manual}
          onCheckedChange={setManual}
          option={[t('tableView.selectDates'), t('tableView.isoString')]}
        />
        <DateSelection type="min" />
        <DateSelection type="max" />
        <Stack>
          {map.map((option, index) => (
            <Button className={s.button} variant='secondary' onClick={option.do} key={index}>{option.text}</Button>
          ))}
        </Stack>
      </UIBanner>
    )
  }

}
