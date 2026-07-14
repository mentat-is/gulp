import { useState, useEffect } from 'react'
import { Stack } from '@/ui/Stack'
import { Input } from '@/ui/Input'
import { Button } from '@/ui/Button'
import * as Dialog from '@radix-ui/react-dialog'
import { Label } from '@/ui/Label'
import { Select } from '@/ui/Select'
import { Checkbox } from '@/ui/Checkbox'
import { Icon } from '@/ui/Icon'
import s from '../styles/AdvancedPluginParams.module.css'
import { cn } from '@/ui/utils'
import { Locale } from '@/locales'

/**
 * MappingFieldData defines the structure for a single field configuration 
 * within a mapping object.
 */
export interface MappingFieldData {
  name: string
  flatten_json?: boolean
  force_type?: string
  multiplier?: number
  is_timestamp?: string
  is_gulp_type?: string
  extra_doc_with_event_code?: string
  ecs?: string[]
  timestamp_format?: string
}

interface MappingFieldPanelProps {
  initialData?: MappingFieldData | null
  onSave: (data: MappingFieldData) => void
  open: boolean
  setOpen: (open: boolean) => void
}

/**
 * MappingFieldPanel is a modal dialog for adding or editing a specific field configuration.
 * It provides inputs for all Gulp-supported field transformations like flattening, 
 * type forcing, multipliers, and ECS field mappings.
 */
export function MappingFieldPanel({ initialData, onSave, open, setOpen }: MappingFieldPanelProps) {
  const { t } = Locale.use()
  const [name, setName] = useState('')
  const [flattenJson, setFlattenJson] = useState(false)
  const [forceType, setForceType] = useState('none')
  const [multiplier, setMultiplier] = useState('')
  const [isTimestamp, setIsTimestamp] = useState('none')
  const [isGulpType, setIsGulpType] = useState('none')
  const [extraDocWithEventCode, setExtraDocWithEventCode] = useState('')
  const [ecsStr, setEcsStr] = useState('')
  const [timestampFormat, setTimestampFormat] = useState('')

  useEffect(() => {
    if (open) {
      if (initialData) {
        setName(initialData.name)
        setFlattenJson(initialData.flatten_json || false)
        setForceType(initialData.force_type || 'none')
        setMultiplier(initialData.multiplier !== undefined && initialData.multiplier !== null ? String(initialData.multiplier) : '')
        setIsTimestamp(initialData.is_timestamp || 'none')
        setIsGulpType(initialData.is_gulp_type || 'none')
        setExtraDocWithEventCode(initialData.extra_doc_with_event_code || '')
        setEcsStr(initialData.ecs?.join(', ') || '')
        setTimestampFormat(initialData.timestamp_format || '')
      } else {
        setName('')
        setFlattenJson(false)
        setForceType('none')
        setMultiplier('')
        setIsTimestamp('none')
        setIsGulpType('none')
        setExtraDocWithEventCode('')
        setEcsStr('')
        setTimestampFormat('')
      }
    }
  }, [open, initialData])

  /**
   * Constructs the MappingFieldData object from local state and performs cleanup 
   * of undefined/empty properties before calling onSave.
   */
  const handleSave = () => {
    const data: MappingFieldData = {
      name,
      flatten_json: flattenJson ? true : undefined,
      force_type: forceType !== 'none' ? forceType : undefined,
      multiplier: multiplier !== '' && !isNaN(Number(multiplier)) ? Number(multiplier) : undefined,
      is_timestamp: isTimestamp !== 'none' ? isTimestamp : undefined,
      is_gulp_type: isGulpType !== 'none' ? isGulpType : undefined,
      extra_doc_with_event_code: extraDocWithEventCode || undefined,
      ecs: ecsStr ? ecsStr.split(',').map(s => s.trim()).filter(Boolean) : undefined,
      timestamp_format: timestampFormat || undefined
    }

    // Clean up undefined properties to keep the resulting JSON clean
    Object.keys(data).forEach(key => {
      if (data[key as keyof MappingFieldData] === undefined) {
        delete data[key as keyof MappingFieldData]
      }
    })

    onSave(data)
    
    // Reset state if it's a new entry (Add mode)
    if (initialData) {
      setOpen(false)
    } else {
      setName('')
      setFlattenJson(false)
      setForceType('none')
      setMultiplier('')
      setIsTimestamp('none')
      setIsGulpType('none')
      setExtraDocWithEventCode('')
      setEcsStr('')
      setTimestampFormat('')
    }
  }

  return (
    <Dialog.Root open={open} onOpenChange={setOpen}>
      <Dialog.Portal>
        <Dialog.Overlay className={s.overlayL3} />
        <Dialog.Content 
          aria-describedby={undefined}
          className={s.contentL3}
        >
        <Stack dir="column" gap={16} ai="stretch">
          <Dialog.Title className={cn(s.titleBase, s.titleS)}>
            {initialData ? t('advancedParams.updateField') : t('advancedParams.addField')}
          </Dialog.Title>
          
          <Input 
            label={t('advancedParams.fieldName')} 
            placeholder={t('advancedParams.fieldNamePlaceholder')} 
            value={name} 
            onChange={(e) => setName(e.target.value)}
            valid={!!name}
          />

          <Stack ai="center" gap={8} style={{ padding: '8px 0' }}>
            <Checkbox id="flatten_json" checked={flattenJson} onCheckedChange={(c) => setFlattenJson(!!c)} />
            <Label htmlFor="flatten_json" value={t('advancedParams.flattenJson')} cursor="pointer" />
          </Stack>

          <Stack dir="column" gap={6} ai="flex-start" data-input>
            <Label value={t('advancedParams.forceType')} />
            <Select.Root value={forceType} onValueChange={setForceType}>
              <Select.Trigger value={forceType} data-no-icon style={{ width: '100%' }}>
                {forceType === 'none' ? t('common.none') : forceType}
              </Select.Trigger>
              <Select.Content>
                <Select.Item value="none">{t('common.none')}</Select.Item>
                <Select.Item value="str">str</Select.Item>
                <Select.Item value="int">int</Select.Item>
                <Select.Item value="float">float</Select.Item>
              </Select.Content>
            </Select.Root>
          </Stack>

          <Input 
            label={t('advancedParams.multiplier')} 
            placeholder={t('advancedParams.multiplierPlaceholder')} 
            type="number"
            value={multiplier} 
            onChange={(e) => setMultiplier(e.target.value)} 
            variant="highlighted"
            icon="ChevronRight" 
          />

          <Stack dir="column" gap={6} ai="flex-start" data-input>
            <Label value={t('advancedParams.isTimestamp')} />
            <Select.Root value={isTimestamp} onValueChange={setIsTimestamp}>
              <Select.Trigger value={isTimestamp} data-no-icon style={{ width: '100%' }}>
                {isTimestamp === 'none' ? t('common.none') : isTimestamp}
              </Select.Trigger>
              <Select.Content>
                <Select.Item value="none">{t('common.none')}</Select.Item>
                <Select.Item value="chrome">chrome</Select.Item>
                <Select.Item value="windows_filetime">windows_filetime</Select.Item>
                <Select.Item value="generic">generic</Select.Item>
              </Select.Content>
            </Select.Root>
          </Stack>

          <Stack dir="column" gap={6} ai="flex-start" data-input>
            <Label value={t('advancedParams.isGulpType')} />
            <Select.Root value={isGulpType} onValueChange={setIsGulpType}>
              <Select.Trigger value={isGulpType} data-no-icon style={{ width: '100%' }}>
                {isGulpType === 'none' ? t('common.none') : isGulpType}
              </Select.Trigger>
              <Select.Content>
                <Select.Item value="none">{t('common.none')}</Select.Item>
                <Select.Item value="context_id">context_id</Select.Item>
                <Select.Item value="context_name">context_name</Select.Item>
                <Select.Item value="source_id">source_id</Select.Item>
                <Select.Item value="source_name">source_name</Select.Item>
              </Select.Content>
            </Select.Root>
          </Stack>

          <Input 
            label={t('advancedParams.extraDocWithEventCode')} 
            placeholder={t('advancedParams.extraDocPlaceholder')} 
            value={extraDocWithEventCode} 
            onChange={(e) => setExtraDocWithEventCode(e.target.value)}
          />
          
          <Input 
            label={t('advancedParams.ecsFieldsComma')} 
            placeholder={t('advancedParams.ecsFieldsPlaceholder')} 
            value={ecsStr} 
            onChange={(e) => setEcsStr(e.target.value)} 
            valid={!!ecsStr}
          />

          <Input 
            label={t('advancedParams.timestampFormat')} 
            placeholder={t('advancedParams.timestampFormatPlaceholder')} 
            value={timestampFormat} 
            onChange={(e) => setTimestampFormat(e.target.value)} 
          />

          <Stack dir="row" gap={8} jc="flex-end" style={{ marginTop: 8 }}>
            <Button 
              variant="secondary" 
              onClick={() => setOpen(false)}
            >
              {t('common.cancel')}
            </Button>
            <Button 
              variant="tertiary" 
              disabled={!name || !ecsStr} 
              onClick={handleSave}
            >
              {initialData ? t('common.update') : t('common.add')}
            </Button>
          </Stack>
        </Stack>
        </Dialog.Content>
      </Dialog.Portal>
    </Dialog.Root>
  )
}
