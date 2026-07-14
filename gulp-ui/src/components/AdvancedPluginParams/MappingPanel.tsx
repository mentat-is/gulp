import { useState, useEffect } from 'react'
import { Stack } from '@/ui/Stack'
import { Input } from '@/ui/Input'
import { Button } from '@/ui/Button'
import * as Dialog from '@radix-ui/react-dialog'
import { Label } from '@/ui/Label'
import { SummaryTable } from './SummaryTable'
import { MappingFieldData, MappingFieldPanel } from './MappingFieldPanel'
import { MappingValueAliasData, MappingValueAliasPanel } from './MappingValueAliasPanel'
import { Separator } from '@/ui/Separator'
import s from '../styles/AdvancedPluginParams.module.css'
import { cn } from '@/ui/utils'
import { Locale } from '@/locales'

/**
 * MappingData represents the structure of a Gulp mapping configuration object,
 * including identity, filtering rules, and nested field/alias transformations.
 */
export interface MappingData {
  id: string
  agent_type?: string
  description?: string
  event_code?: string
  exclude?: string[]
  include?: string[]
  default_context?: string
  default_source?: string
  default_encoding?: string
  fields?: Record<string, Partial<MappingFieldData>>
  value_aliases?: Record<string, { default: Record<string, string> }>
}

interface MappingPanelProps {
  initialData?: MappingData | null
  onSave: (data: MappingData) => void
  open: boolean
  setOpen: (open: boolean) => void
}

/**
 * MappingPanel is a high-level modal component for managing a MappingData object.
 * It coordinates multiple sub-panels for field definitions and value aliases,
 * providing recursive configuration capabilities.
 */
export function MappingPanel({ initialData, onSave, open, setOpen }: MappingPanelProps) {
  const { t } = Locale.use()
  const [id, setId] = useState('')
  const [agentType, setAgentType] = useState('')
  const [description, setDescription] = useState('')
  const [eventCode, setEventCode] = useState('')
  const [excludeStr, setExcludeStr] = useState('')
  const [includeStr, setIncludeStr] = useState('')
  const [defaultContext, setDefaultContext] = useState('')
  const [defaultSource, setDefaultSource] = useState('')
  const [defaultEncoding, setDefaultEncoding] = useState('')

  const [fields, setFields] = useState<MappingFieldData[]>([])
  const [aliases, setAliases] = useState<MappingValueAliasData[]>([])

  // State for sub-panels
  const [isFieldPanelOpen, setIsFieldPanelOpen] = useState(false)
  const [editingField, setEditingField] = useState<MappingFieldData | null>(null)
  const [editingFieldIndex, setEditingFieldIndex] = useState<number | null>(null)

  const [isAliasPanelOpen, setIsAliasPanelOpen] = useState(false)
  const [editingAlias, setEditingAlias] = useState<MappingValueAliasData | null>(null)
  const [editingAliasIndex, setEditingAliasIndex] = useState<number | null>(null)

  // Hydrating the panel state from the initialData object when editing an existing mapping.
  // This logic reconstructs the flat lists for Fields and Aliases from the nested JSON.
  useEffect(() => {
    if (open) {
      if (initialData) {
        setId(initialData.id)
        setAgentType(initialData.agent_type || '')
        setDescription(initialData.description || '')
        setEventCode(initialData.event_code || '')
        setExcludeStr(initialData.exclude?.join(', ') || '')
        setIncludeStr(initialData.include?.join(', ') || '')
        setDefaultContext(initialData.default_context || '')
        setDefaultSource(initialData.default_source || '')
        setDefaultEncoding(initialData.default_encoding || '')

        // Parse fields into a displayable list
        const parsedFields: MappingFieldData[] = []
        if (initialData.fields) {
          Object.entries(initialData.fields).forEach(([fieldName, fieldData]) => {
            parsedFields.push({ name: fieldName, ...fieldData })
          })
        }
        setFields(parsedFields)

        // Parse aliases into a displayable list
        const parsedAliases: MappingValueAliasData[] = []
        if (initialData.value_aliases) {
          Object.entries(initialData.value_aliases).forEach(([fieldName, valueMap]) => {
            if (valueMap.default) {
              Object.entries(valueMap.default).forEach(([oldValue, newValue]) => {
                parsedAliases.push({ field: fieldName, oldValue, newValue })
              })
            }
          })
        }
        setAliases(parsedAliases)
      } else {
        // Reset state for "Create Mapping" mode
        setId('')
        setAgentType('')
        setDescription('')
        setEventCode('')
        setExcludeStr('')
        setIncludeStr('')
        setDefaultContext('')
        setDefaultSource('')
        setDefaultEncoding('')
        setFields([])
        setAliases([])
      }
    }
  }, [open, initialData])

  const handleSaveField = (fieldData: MappingFieldData) => {
    if (editingFieldIndex !== null) {
      const newFields = [...fields]
      newFields[editingFieldIndex] = fieldData
      setFields(newFields)
    } else {
      setFields([...fields, fieldData])
    }
  }

  const handleDeleteField = (_: MappingFieldData, idx: number) => {
    setFields(fields.filter((_, i) => i !== idx))
  }

  const handleSaveAlias = (aliasData: MappingValueAliasData) => {
    if (editingAliasIndex !== null) {
      const newAliases = [...aliases]
      newAliases[editingAliasIndex] = aliasData
      setAliases(newAliases)
    } else {
      setAliases([...aliases, aliasData])
    }
  }

  const handleDeleteAlias = (_: MappingValueAliasData, idx: number) => {
    setAliases(aliases.filter((_, i) => i !== idx))
  }

  /**
   * Transforms the UI list-based state back into the structured MappingData object.
   * It aggregates individual field and alias entries into nested Record objects.
   *
   * @returns void
   */
  const handleSave = () => {
    const mappingId = id.trim()
    const fieldsObj: Record<string, Partial<MappingFieldData>> = {}
    fields.forEach(f => {
      const { name, ...rest } = f
      if (Object.keys(rest).length > 0) {
        fieldsObj[name] = rest
      }
    })

    const aliasesObj: Record<string, { default: Record<string, string> }> = {}
    aliases.forEach(a => {
      if (!aliasesObj[a.field]) {
        aliasesObj[a.field] = { default: {} }
      }
      aliasesObj[a.field].default[a.oldValue] = a.newValue
    })

    const data: MappingData = {
      id: mappingId,
      agent_type: agentType || undefined,
      description: description || undefined,
      event_code: eventCode || undefined,
      exclude: excludeStr ? excludeStr.split(',').map(s => s.trim()).filter(Boolean) : undefined,
      include: includeStr ? includeStr.split(',').map(s => s.trim()).filter(Boolean) : undefined,
      default_context: defaultContext || undefined,
      default_source: defaultSource || undefined,
      default_encoding: defaultEncoding || undefined,
      fields: Object.keys(fieldsObj).length > 0 ? fieldsObj : undefined,
      value_aliases: Object.keys(aliasesObj).length > 0 ? aliasesObj : undefined
    }

    onSave(data)
    setOpen(false)
  }

  return (
    <Dialog.Root open={open} onOpenChange={setOpen}>
      <Dialog.Portal>
        <Dialog.Overlay className={s.overlayL2} />
        <Dialog.Content 
          aria-describedby={undefined}
          className={s.contentL2}
        >
          <Stack dir="column" gap={16} ai="stretch">
          <Dialog.Title className={cn(s.titleBase, s.titleM)}>
            {initialData ? t('advancedParams.updateMappingObject') : t('advancedParams.createMappingObject')}
          </Dialog.Title>
          
          <Stack dir="row" gap={16} ai="flex-start">
            <Stack dir="column" gap={12} ai="stretch" style={{ flex: 1 }}>
              <Input label={t('advancedParams.mappingId')} value={id} onChange={(e) => setId(e.target.value)} valid={!!id} disabled={!!initialData} />
              <Input label={t('advancedParams.agentType')} value={agentType} onChange={(e) => setAgentType(e.target.value)}  />
              <Input label={t('common.description')} value={description} onChange={(e) => setDescription(e.target.value)}  />
              <Input label={t('advancedParams.eventCode')} value={eventCode} onChange={(e) => setEventCode(e.target.value)}  />
              <Input label={t('advancedParams.excludeComma')} value={excludeStr} onChange={(e) => setExcludeStr(e.target.value)}  />
            </Stack>
            <Stack dir="column" gap={12} ai="stretch" style={{ flex: 1 }}>
              <Input label={t('advancedParams.includeComma')} value={includeStr} onChange={(e) => setIncludeStr(e.target.value)}  />
              <Input label={t('advancedParams.defaultContext')} value={defaultContext} onChange={(e) => setDefaultContext(e.target.value)}  />
              <Input label={t('advancedParams.defaultSource')} value={defaultSource} onChange={(e) => setDefaultSource(e.target.value)}  />
              <Input label={t('advancedParams.defaultEncoding')} value={defaultEncoding} onChange={(e) => setDefaultEncoding(e.target.value)}  />
            </Stack>
          </Stack>

          <Separator style={{ margin: '8px 0' }} />

          <Stack dir="row" jc="space-between" ai="center">
            <Label value={t('common.fields')} className={s.labelBold} />
            <Button variant="secondary" onClick={() => { setEditingField(null); setEditingFieldIndex(null); setIsFieldPanelOpen(true) }}>
              {t('advancedParams.addField')}
            </Button>
            <MappingFieldPanel 
              open={isFieldPanelOpen} 
              setOpen={setIsFieldPanelOpen} 
              onSave={handleSaveField} 
              initialData={editingField} 
            />
          </Stack>
          <SummaryTable 
            columns={[
              { key: 'name', label: t('common.name') },
              { key: 'force_type', label: t('advancedParams.forceType') },
              { key: 'is_timestamp', label: t('advancedParams.isTimestamp') },
              { key: 'ecs', label: 'ECS' }
            ]} 
            data={fields}
            onEdit={(item, index) => {
              setEditingField(item)
              setEditingFieldIndex(index)
              setIsFieldPanelOpen(true)
            }}
            onDelete={handleDeleteField}
          />

          <Separator style={{ margin: '8px 0' }} />

          <Stack dir="row" jc="space-between" ai="center">
            <Label value={t('advancedParams.valueAliases')} className={s.labelBold} />
            <Button variant="secondary" onClick={() => { setEditingAlias(null); setEditingAliasIndex(null); setIsAliasPanelOpen(true) }}>
              {t('advancedParams.addValueAlias')}
            </Button>
            <MappingValueAliasPanel 
              open={isAliasPanelOpen} 
              setOpen={setIsAliasPanelOpen} 
              onSave={handleSaveAlias} 
              initialData={editingAlias} 
            />
          </Stack>
          <SummaryTable 
            columns={[
              { key: 'field', label: t('common.field') },
              { key: 'oldValue', label: t('advancedParams.oldValue') },
              { key: 'newValue', label: t('advancedParams.newValue') }
            ]} 
            data={aliases}
            onEdit={(item, index) => {
              setEditingAlias(item)
              setEditingAliasIndex(index)
              setIsAliasPanelOpen(true)
            }}
            onDelete={handleDeleteAlias}
          />

          <Stack dir="row" gap={8} jc="flex-end" style={{ marginTop: 16 }}>
            <Button 
              variant="secondary" 
              onClick={() => setOpen(false)}
            >
              {t('common.cancel')}
            </Button>
            <Button 
              variant="glass" 
              icon={initialData ? "Save" : "Plus"} 
              disabled={!id.trim()}
              onClick={handleSave}
            >
              {initialData ? t('advancedParams.updateMapping') : t('advancedParams.addMapping')}
            </Button>
          </Stack>
        </Stack>
        </Dialog.Content>
      </Dialog.Portal>
    </Dialog.Root>
  )
}
