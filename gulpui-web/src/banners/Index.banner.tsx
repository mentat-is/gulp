import { Banner } from '@/ui/Banner'
import { Locale } from '@/locales'

export namespace IndexBanner {
  export type Props = Banner.Props
}

export function IndexBanner({ ...props }: IndexBanner.Props) {
  const { t } = Locale.use()
  return <Banner title={t('index.selectIndex')} {...props}></Banner>
}
