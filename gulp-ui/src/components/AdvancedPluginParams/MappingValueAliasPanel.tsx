import { useState, useEffect } from 'react'
import { Stack } from '@/ui/Stack'
import { Input } from '@/ui/Input'
import { Button } from '@/ui/Button'
import * as Dialog from '@radix-ui/react-dialog'
import { Label } from '@/ui/Label'
import s from '../styles/AdvancedPluginParams.module.css'
import { cn } from '@/ui/utils'
import { Locale } from '@/locales'

/**
 * MappingValueAliasData represents a value translation rule for a specific field.
 */
export interface MappingValueAliasData {
  field: string
  oldValue: string
  newValue: string
}

interface MappingValueAliasPanelProps {
  initialData?: MappingValueAliasData | null
  onSave: (data: MappingValueAliasData) => void
  open: boolean
  setOpen: (open: boolean) => void
}

/**
 * MappingValueAliasPanel is a modal dialog for configuring value translations.
 * It maps an 'oldValue' found in the source data to a 'newValue' in the output 
 * for a designated field.
 */
export function MappingValueAliasPanel({ initialData, onSave, open, setOpen }: MappingValueAliasPanelProps) {
  const { t } = Locale.use()
  const [field, setField] = useState('')
  const [oldValue, setOldValue] = useState('')
  const [newValue, setNewValue] = useState('')

  useEffect(() => {
    if (open) {
      if (initialData) {
        setField(initialData.field)
        setOldValue(initialData.oldValue)
        setNewValue(initialData.newValue)
      } else {
        setField('')
        setOldValue('')
        setNewValue('')
      }
    }
  }, [open, initialData])

  /**
   * Constructs the MappingValueAliasData and triggers the save callback.
   */
  const handleSave = () => {
    onSave({ field, oldValue, newValue })
    
    // Reset state for "Add" mode
    if (initialData) {
      setOpen(false)
    } else {
      setField('')
      setOldValue('')
      setNewValue('')
    }
  }

  return (
    <Dialog.Root open={open} onOpenChange={setOpen}>
      <Dialog.Portal>
        <Dialog.Overlay className={s.overlayL3} />
        <Dialog.Content 
          aria-describedby={undefined}
          className={s.contentL3Small}
        >
        <Stack dir="column" gap={16} ai="stretch">
          <Dialog.Title className={cn(s.titleBase, s.titleS)}>
            {initialData ? t('advancedParams.updateValueAlias') : t('advancedParams.addValueAlias')}
          </Dialog.Title>
          <Input 
            label={t('advancedParams.fieldName')} 
            placeholder={t('advancedParams.valueAliasFieldPlaceholder')} 
            value={field} 
            onChange={(e) => setField(e.target.value)} 
            
          />
          
          <Input 
            label={t('advancedParams.oldValue')} 
            placeholder={t('advancedParams.originalStringPlaceholder')} 
            value={oldValue} 
            onChange={(e) => setOldValue(e.target.value)} 
             
          />
          
          <Input 
            label={t('advancedParams.newValue')} 
            placeholder={t('advancedParams.replacementStringPlaceholder')} 
            value={newValue} 
            onChange={(e) => setNewValue(e.target.value)} 
             
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
              disabled={!field || !oldValue || !newValue} 
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
