// .vitepress/theme/index.js
import DefaultTheme from 'vitepress/theme'
import './custom.css'
import { onMounted } from 'vue'

export default {
  extends: DefaultTheme,
  setup() {
    onMounted(() => {
      // Load custom JavaScript
      const script = document.createElement('script')
      script.src = '/firehose/custom.js'
      document.body.appendChild(script)
    })
  }
}
