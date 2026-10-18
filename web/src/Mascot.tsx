export type Mood =
  | 'smile'
  | 'joy'
  | 'cry'
  | 'dejected'
  | 'terrified'
  | 'angry'
  | 'spoiled'
  | 'cute'
  | 'plead'
  | 'knock'
  | 'facepalm'
  | 'tongue'

/// 项目吉祥物（存储桶小家伙）的 `<img>` 封装。
/// 12 种形态与 `docs/pet-expressions/*.svg` 同源（去掉标签与底色后裁成 184×180）；
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
