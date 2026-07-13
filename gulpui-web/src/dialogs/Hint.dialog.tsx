import { Application } from "@/context/Application.context";
import { Button } from "@/ui/Button";
import { Stack } from "@/ui/Stack";
import s from './styles/HintDialog.module.css'
import { Keyboard } from "@/ui/Keyboard";
import { useEffect } from "react";
import { Locale } from "@/locales";

export namespace Hint {
  export namespace Dialog {
    export interface Props {
      onClose?: () => void
    }
  }

  export function Dialog({ onClose }: Hint.Dialog.Props) {
    const { banner, currentDocument } = Application.use()
    const { t } = Locale.use()

    useEffect(() => {
      const handleDialogClose = (event: KeyboardEvent) => {
        if (event.key === 'Escape' && !banner) {
          onClose?.()
        }
      }

      currentDocument.addEventListener('keydown', handleDialogClose)

      return () => {
        currentDocument.removeEventListener('keydown', handleDialogClose)
      }
    }, [banner, currentDocument, onClose])

    return (
      <div className={s.overlay} onMouseDown={() => onClose?.()}>
        <Stack className={s.modal} dir='column' gap={16} onMouseDown={(event) => event.stopPropagation()}>
          <Stack jc='space-between' ai='center' gap={12}>
            <h2 className={s.title}>{t("hint.title")}</h2>
            <Button
              variant='secondary'
              size='sm'
              icon='X'
              title={t("hint.close")}
              onClick={() => onClose?.()}
            />
          </Stack>
          <ul className={s.list}>
            <li>{t("hint.use")} <Keyboard>{t("hint.leftMouseClick")}</Keyboard> {t("hint.leftClick")}</li>
            <li>{t("hint.use")} <Keyboard>{t("hint.rightMouseClick")}</Keyboard> {t("hint.rightClick")}</li>
            <li>{t("hint.press")} <Keyboard>=</Keyboard> {t("hint.adjustView")}</li>
            <li>{t("hint.press")} <Keyboard>+</Keyboard> {t("hint.or")} <Keyboard>-</Keyboard> {t("hint.zoom")}</li>
            <li>{t("hint.press")} <Keyboard meta /> {t("hint.toggleMagnifier")}</li>
            <li>{t("hint.hold")} <Keyboard>Alt</Keyboard> {t("hint.or")} <Keyboard alt /> {t("hint.selectFrame")}</li>
            <li>{t("hint.press")} <Keyboard>Page Up</Keyboard> / <Keyboard>Page Down</Keyboard> {t("hint.or")} <Keyboard>←</Keyboard> / <Keyboard>↑</Keyboard> / <Keyboard>↓</Keyboard> / <Keyboard>→</Keyboard> {t("hint.scrollCanvas")}</li>
            <li>{t("hint.press")} <Keyboard>Esc</Keyboard> {t("hint.closeBanner")}</li>
            <li>{t("hint.press")} <Keyboard>A</Keyboard> {t("hint.or")} <Keyboard>D</Keyboard> {t("hint.navigateEvents")}</li>
          </ul>
        </Stack>
      </div>
    )
  }
}
