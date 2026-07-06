import { defineConfig } from 'vitepress'

export default defineConfig({
  title: 'Firehose',
  description: 'Type-Safe Event Processing for Go',
  base: '/firehose/',
  ignoreDeadLinks: true,
  
  themeConfig: {
    logo: '/logo.svg',
    
    nav: [
      { text: 'Guide', link: '/guide/introduction' },
      { text: 'API', link: '/api/' },
      { text: 'Examples', link: '/examples/' },
      {
        text: 'v1.0.0',
        items: [
          { text: 'Changelog', link: 'https://github.com/emad-elsaid/firehose/releases' },
          { text: 'Contributing', link: 'https://github.com/emad-elsaid/firehose/blob/main/CONTRIBUTING.md' }
        ]
      }
    ],

    sidebar: {
      '/guide/': [
        {
          text: 'Introduction',
          items: [
            { text: 'What is Firehose?', link: '/guide/introduction' },
            { text: 'Quick Start', link: '/guide/quick-start' },
          ]
        },
        {
          text: 'Core Concepts',
          items: [
            { text: 'Events & Rules', link: '/guide/concepts' },
            { text: 'Built-in Components', link: '/guide/components' },
            { text: 'Middleware', link: '/guide/middleware' },
            { text: 'Hierarchical Rules', link: '/guide/hierarchical-rules' },
          ]
        },
        {
          text: 'Advanced',
          items: [
            { text: 'Custom Components', link: '/guide/custom-components' },
            { text: 'Testing', link: '/guide/testing' },
            { text: 'Environment Rules', link: '/guide/environments' },
            { text: 'Best Practices', link: '/guide/best-practices' },
          ]
        }
      ],
      
      '/api/': [
        {
          text: 'API Reference',
          items: [
            { text: 'Overview', link: '/api/' },
            { text: 'Core Types', link: '/api/core' },
            { text: 'Conditions', link: '/api/conditions' },
            { text: 'Actions', link: '/api/actions' },
            { text: 'Destinations', link: '/api/destinations' },
            { text: 'Sources', link: '/api/sources' },
            { text: 'Middleware', link: '/api/middleware' },
          ]
        }
      ],
      
      '/examples/': [
        {
          text: 'Examples',
          items: [
            { text: 'Overview', link: '/examples/' },
            { text: 'HTTP Server', link: '/examples/http-server' },
            { text: 'Message Queue', link: '/examples/message-queue' },
            { text: 'System Monitor', link: '/examples/system-monitor' },
            { text: 'Event-Driven Microservice', link: '/examples/microservice' },
          ]
        }
      ]
    },

    socialLinks: [
      { icon: 'github', link: 'https://github.com/emad-elsaid/firehose' }
    ],

    footer: {
      message: 'Released under the MIT License.',
      copyright: 'Copyright © 2024-present Emad Elsaid'
    },

    search: {
      provider: 'local'
    },

    editLink: {
      pattern: 'https://github.com/emad-elsaid/firehose/edit/main/website/docs/:path',
      text: 'Edit this page on GitHub'
    }
  }
})
