export type Mood = 'happy' | 'smug' | 'dejected' | 'cry' | 'shocked'

/// 项目吉祥物（存储桶小家伙）的 `<img>` 封装。
/// sprite 在 `web/public/mascot/*.svg`，设计稿见 `docs/assets/mascot*.svg`；
/// SVG 自带的 SMIL 动画（眨眼、落泪、抖动等）经 `<img>` 引用时在浏览器里照常播放。
export default function Mascot({
  mood,
  size = 96,
  alt = '',
}: {
  mood: Mood
  size?: number
  alt?: string
}) {
  return (
    <img
      src={`/mascot/${mood}.svg`}
      width={size}
      height={size}
      alt={alt}
      draggable={false}
      style={{ verticalAlign: 'middle' }}
    />
  )
}
