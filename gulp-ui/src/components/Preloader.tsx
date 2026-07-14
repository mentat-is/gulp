import { Stack } from '@/ui/Stack'
import s from './styles/Preloader.module.css'
import { Shimmer } from '@/ui/Shimmer'
import { Spinner } from '@/ui/Spinner'
import { Logo } from './Logo'
import { Locale } from '@/locales'

export function Preloader() {
  const { t } = Locale.use()
  return (
    <Stack className={s.main} ai='center' jc='center' dir='column' gap={24}>
      <Logo style={{ marginBottom: '7.6%' }} />
      <Stack dir='column' gap={12} style={{ marginBottom: '1%' }}>
        <Shimmer duration={2} as='h1' color='var(--gray-800)'>{t('preloader.title')}</Shimmer>
        <Shimmer color='var(--gray-500)'>{t('preloader.subtitle')}</Shimmer>
      </Stack>
      <Spinner color='var(--accent)' size={32} />
    </Stack>
  )
}
