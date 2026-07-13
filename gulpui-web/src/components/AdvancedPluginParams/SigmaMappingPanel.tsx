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
 * SigmaMappingData defines the structure for service-specific sigma detection 
 * rules within the Gulp configuration.
 */
export interface SigmaMappingData {
  name: string
  service_field: string
  service_values: string[]
}

interface SigmaMappingPanelProps {
  initialData?: SigmaMappingData | null
  onSave: (data: SigmaMappingData) => void
  open: boolean
  setOpen: (open: boolean) => void
}

/**
 * SigmaMappingPanel is a modal dialog for adding or editing Sigma mapping entries.
 * It manages the association between a service name and its corresponding 
 * service field and filter values.
 */
export function SigmaMappingPanel({ initialData, onSave, open, setOpen }: SigmaMappingPanelProps) {
  const { t } = Locale.use()
  const [name, setName] = useState('')
  const [serviceField, setServiceField] = useState('')
  const [serviceValuesStr, setServiceValuesStr] = useState('')

  useEffect(() => {
    if (open) {
      if (initialData) {
        setName(initialData.name)
        setServiceField(initialData.service_field || '')
        setServiceValuesStr(initialData.service_values?.join(', ') || '')
      } else {
        setName('')
        setServiceField('')
        setServiceValuesStr('')
      }
    }
  }, [open, initialData])

  /**
   * Constructs the SigmaMappingData object from local state, converting 
   * the comma-separated values string into an array.
   */
  const handleSave = () => {
    onSave({ 
      name, 
      service_field: serviceField, 
      service_values: serviceValuesStr.split(',').map(s => s.trim()).filter(Boolean) 
    })
    
    // Reset state for "Add" mode
    if (initialData) {
      setOpen(false)
    } else {
      setName('')
      setServiceField('')
      setServiceValuesStr('')
    }
  }

  return (
    <Dialog.Root open={open} onOpenChange={setOpen}>
      <Dialog.Portal>
        <Dialog.Overlay className={s.overlayL2} />
        <Dialog.Content 
          aria-describedby={undefined}
          className={s.contentL2Small}
        >
        <Stack dir="column" gap={16} ai="stretch">
          <Dialog.Title className={cn(s.titleBase, s.titleS)}>
            {initialData ? t('advancedParams.updateSigmaMapping') : t('advancedParams.addSigmaMapping')}
          </Dialog.Title>
          
          <Input 
            label={t('advancedParams.mappingKeyName')} 
            placeholder={t('advancedParams.sigmaNamePlaceholder')} 
            value={name} 
            onChange={(e) => setName(e.target.value)} 
             
          />
          
          <Input 
            label={t('advancedParams.serviceField')} 
            placeholder={t('advancedParams.serviceFieldPlaceholder')} 
            value={serviceField} 
            onChange={(e) => setServiceField(e.target.value)} 
             
          />
          
          <Input 
            label={t('advancedParams.serviceValuesComma')} 
            placeholder={t('advancedParams.serviceValuesPlaceholder')} 
            value={serviceValuesStr} 
            onChange={(e) => setServiceValuesStr(e.target.value)} 
             
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
              disabled={!name || !serviceField || !serviceValuesStr} 
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
