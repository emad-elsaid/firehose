// .vitepress/theme/index.js
import DefaultTheme from 'vitepress/theme'
import './custom.css'
import { onMounted } from 'vue'
import { enhanceAppWithTabs } from 'vitepress-plugin-tabs/client'

export default {
  extends: DefaultTheme,
  enhanceApp({ app }) {
    enhanceAppWithTabs(app)
  },
  setup() {
    onMounted(() => {
      const script = document.createElement('script')
      script.src = '/firehose/custom.js'
      document.body.appendChild(script)
    })
  }
}
