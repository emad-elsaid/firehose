<template>
  <div class="firehose-home">
    <div class="hero" ref="heroRef">
      <div class="container">
        <div class="hero-content">
          <h1 class="hero-title">
            <span class="gradient-text">Type-Safe Event Processing</span>
            <span class="subtitle">for Go</span>
          </h1>
          <p class="hero-description">
            Build composable event pipelines with conditional execution, hierarchical rules,
            and middleware support. Event-driven architecture that the compiler can verify.
            Choose the naming convention that fits your team: SQL, BDD, Kafka Streams,
            or MapReduce.
          </p>
          <div class="hero-actions">
            <a href="/firehose/guide/quick-start" class="btn btn-primary">Get Started</a>
            <a href="https://github.com/emad-elsaid/firehose" target="_blank" class="btn btn-secondary">
              View on GitHub
            </a>
          </div>
        </div>
        <div class="hero-code">
          <div class="hero-tabs">
            <button
              v-for="tab in tabs"
              :key="tab.name"
              :class="['hero-tab', { active: activeTab === tab.name }]"
              @click="activeTab = tab.name"
            >
              {{ tab.name }}
            </button>
          </div>
          <pre><code v-html="activeTabCode"></code></pre>
          <p class="hero-description" style="margin-top: 0.75rem; font-size: 0.95rem;">
            One pipeline, four naming conventions: <code>SQL</code>,
            <code>BDD (Given-When-Then)</code>, <code>Kafka Streams</code>, or
            <code>MapReduce</code>.
          </p>
        </div>
      </div>
    </div>

    <section class="section problem-solution">
      <div class="container">
        <div class="two-column">
          <div class="column problem">
            <div class="icon">⚠️</div>
            <h2>The Problem</h2>
            <p>
              Event handling scattered across codebases. Difficult to test, hard to modify, 
              impossible to compose. Side effects mixed with business logic. No reusability.
            </p>
          </div>
          <div class="column solution">
            <div class="icon">✨</div>
            <h2>The Solution</h2>
            <p>
              Declarative event pipelines with type safety. Composable components. 
              Hierarchical rules with inheritance. Middleware for cross-cutting concerns.
            </p>
          </div>
        </div>
      </div>
    </section>

    <section id="features" class="section features">
      <div class="container">
        <h2 class="section-title">Features</h2>
        <div class="feature-grid">
          <div class="feature-card">
            <div class="feature-icon">🔒</div>
            <h3>Type-Safe Pipelines</h3>
            <p>Generic types ensure compile-time correctness from source to destination. The compiler verifies your event flows.</p>
          </div>
          <div class="feature-card">
            <div class="feature-icon">🎯</div>
            <h3>Zero Dependencies</h3>
            <p>Any Go type works as an event. No interface requirements. Works seamlessly with existing types.</p>
          </div>
          <div class="feature-card">
            <div class="feature-icon">📋</div>
            <h3>Declarative Conditions</h3>
            <p>Boolean expressions for event filtering. Rich operators for numbers, strings, and collections.</p>
          </div>
          <div class="feature-card">
            <div class="feature-icon">🔌</div>
            <h3>Unified Middleware</h3>
            <p>Single interface for callbacks, transformations, and destinations. Composable cross-cutting concerns.</p>
          </div>
          <div class="feature-card">
            <div class="feature-icon">⚡</div>
            <h3>Source Fanout</h3>
            <p>Shared sources start once and distribute events to all registered rules automatically.</p>
          </div>
        </div>
      </div>
    </section>

    <section id="quickstart" class="section quickstart">
      <div class="container">
        <h2 class="section-title">Quick Start</h2>
        <div class="install-box">
          <span class="install-label">Install via Go modules</span>
          <pre><code>go get github.com/emad-elsaid/firehose</code></pre>
        </div>
        
        <div class="code-example">
          <h3>Hello World: Timer Events</h3>
          <pre><code><span class="keyword">package</span> main

<span class="keyword">import</span> (
    <span class="string">"context"</span>
    <span class="string">"fmt"</span>
    <span class="string">"time"</span>
    
    fh <span class="string">"github.com/emad-elsaid/firehose"</span>
    <span class="string">"github.com/emad-elsaid/firehose/condition"</span>
)

<span class="keyword">type</span> <span class="type">Tick</span> <span class="keyword">struct</span> {
    <span class="field">Time</span> <span class="type">time.Time</span>
}

<span class="keyword">func</span> (t <span class="type">Tick</span>) <span class="function">Get</span>(key <span class="type">string</span>) (<span class="keyword">any</span>, <span class="type">error</span>) {
    <span class="keyword">if</span> key == <span class="string">"hour"</span> {
        <span class="keyword">return</span> t.Time.<span class="function">Hour</span>(), <span class="keyword">nil</span>
    }
    <span class="keyword">return</span> <span class="keyword">nil</span>, <span class="function">fmt.Errorf</span>(<span class="string">"unknown: %s"</span>, key)
}

<span class="keyword">func</span> <span class="function">main</span>() {
    ctx := context.<span class="function">Background</span>()
    
    rule := &fh.<span class="type">SQLRule</span>[<span class="type">Tick</span>, <span class="type">string</span>]{
        <span class="field">ID</span>:     <span class="string">"business_hours"</span>,
        <span class="field">Select</span>: <span class="function">FormatTime</span>{},
        <span class="field">Into</span>:   <span class="function">Printer</span>{},
        <span class="field">From</span>:   <span class="function">Timer</span>{<span class="field">Interval</span>: <span class="number">1</span> * time.Second},
        <span class="field">Where</span>:  condition.<span class="function">Cond</span>[<span class="type">Tick</span>](<span class="string">"hour >= 9 and hour < 17"</span>),
    }
    
    head, _ := fh.<span class="function">Add</span>(ctx, <span class="keyword">nil</span>, rule)
    fh.<span class="function">Start</span>(ctx, head, <span class="keyword">nil</span>)
    fh.<span class="function">Wait</span>(head, <span class="keyword">nil</span>)
}</code></pre>
        </div>
      </div>
    </section>

    <section id="concepts" class="section concepts">
      <div class="container">
        <h2 class="section-title">Core Concepts</h2>
        <div class="concept-grid">
          <div class="concept-card">
            <div class="concept-header">
              <span class="concept-number">01</span>
              <h3>Event Source</h3>
            </div>
            <p>Produces events of a specific type. HTTP servers, message queues, timers, file watchers.</p>
            <div class="concept-code">
              <code><span class="keyword">type</span> <span class="type">Source</span>[T <span class="keyword">any</span>] <span class="keyword">interface</span></code>
            </div>
          </div>
          
          <div class="concept-card">
            <div class="concept-header">
              <span class="concept-number">02</span>
              <h3>Condition</h3>
            </div>
            <p>Optional filter evaluated against event attributes. Boolean expressions or custom logic.</p>
            <div class="concept-code">
              <code><span class="keyword">type</span> <span class="type">Condition</span>[I <span class="keyword">any</span>] <span class="keyword">interface</span></code>
            </div>
          </div>
          
          <div class="concept-card">
            <div class="concept-header">
              <span class="concept-number">03</span>
              <h3>Transformation</h3>
            </div>
            <p>Converts input events to output events. Business logic, enrichment, validation.</p>
            <div class="concept-code">
              <code><span class="keyword">type</span> <span class="type">Action</span>[I, O <span class="keyword">any</span>] <span class="keyword">interface</span></code>
            </div>
          </div>
          
          <div class="concept-card">
            <div class="concept-header">
              <span class="concept-number">04</span>
              <h3>Destination</h3>
            </div>
            <p>Handles output events. Database writes, API calls, message publishing, logging.</p>
            <div class="concept-code">
              <code><span class="keyword">type</span> <span class="type">Destination</span>[T <span class="keyword">any</span>] <span class="keyword">interface</span></code>
            </div>
          </div>
        </div>
      </div>
    </section>

    <section class="section pipeline">
      <div class="container">
        <h2 class="section-title">Event Processing Pipeline</h2>
        <div class="pipeline-diagram">
          <div class="pipeline-step">
            <div class="step-icon">📡</div>
            <div class="step-label">Source</div>
            <div class="step-desc">Emit Event</div>
          </div>
          <div class="pipeline-arrow">→</div>
          <div class="pipeline-step">
            <div class="step-icon">🔍</div>
            <div class="step-label">Condition</div>
            <div class="step-desc">Filter Input</div>
          </div>
          <div class="pipeline-arrow">→</div>
          <div class="pipeline-step">
            <div class="step-icon">⚙️</div>
            <div class="step-label">Action</div>
            <div class="step-desc">Transform</div>
          </div>
          <div class="pipeline-arrow">→</div>
          <div class="pipeline-step">
            <div class="step-icon">🔍</div>
            <div class="step-label">Condition</div>
            <div class="step-desc">Filter Output</div>
          </div>
          <div class="pipeline-arrow">→</div>
          <div class="pipeline-step">
            <div class="step-icon">📤</div>
            <div class="step-label">Sink</div>
            <div class="step-desc">Consume</div>
          </div>
        </div>
      </div>
    </section>

    <section class="section use-cases">
      <div class="container">
        <h2 class="section-title">Use Cases</h2>
        <div class="use-case-grid">
          <div class="use-case">
            <h4>🌐 Microservices</h4>
            <p>HTTP routing, gRPC streams, WebSocket distribution</p>
          </div>
          <div class="use-case">
            <h4>📊 Stream Processing</h4>
            <p>Kafka consumers, real-time chat, log aggregation</p>
          </div>
          <div class="use-case">
            <h4>🔍 System Monitoring</h4>
            <p>Process tracking, file watching, metric collection</p>
          </div>
          <div class="use-case">
            <h4>🤖 Automation</h4>
            <p>Workflow orchestration, rule engines, ETL pipelines</p>
          </div>
          <div class="use-case">
            <h4>🎮 Interactive Systems</h4>
            <p>Game input, UI events, hardware integration</p>
          </div>
          <div class="use-case">
            <h4>📡 Event-Driven Apps</h4>
            <p>CQRS, event sourcing, saga patterns</p>
          </div>
        </div>
      </div>
    </section>

    <section class="section cta">
      <div class="container">
        <div class="cta-content">
          <h2>Ready to Build Type-Safe Pipelines?</h2>
          <p>Start building composable, testable event processing systems in Go</p>
          <div class="cta-actions">
            <a href="https://github.com/emad-elsaid/firehose" target="_blank" class="btn btn-primary">
              Get Started on GitHub
            </a>
            <a href="https://pkg.go.dev/github.com/emad-elsaid/firehose" target="_blank" class="btn btn-secondary">
              View Documentation
            </a>
          </div>
        </div>
      </div>
    </section>
  </div>
</template>

<script setup>
import { computed, onMounted, ref } from 'vue'

const heroRef = ref(null)
const activeTab = ref('SQL')

const tabs = [
  { name: 'SQL', code: `type <span class="type">SQLRule</span>[I, O <span class="keyword">any</span>] <span class="keyword">struct</span> {
    <span class="field">ID</span>     <span class="type">string</span>         <span class="comment">// Unique ID</span>
    <span class="field">Select</span> <span class="type">Action</span>[I, O]   <span class="comment">// Transform</span>
    <span class="field">Into</span>   <span class="type">Destination</span>[O] <span class="comment">// Output</span>
    <span class="field">From</span>   <span class="type">Source</span>[I]      <span class="comment">// Event source</span>
    <span class="field">Where</span>  <span class="type">Condition</span>[I]   <span class="comment">// Input condition</span>
    <span class="field">Having</span> <span class="type">Condition</span>[O]  <span class="comment">// Output condition</span>
}` },
  { name: 'Scenario', code: `type <span class="type">ScenarioRule</span>[I, O <span class="keyword">any</span>] <span class="keyword">struct</span> {
    <span class="field">Give</span>        <span class="type">Source</span>[I]      <span class="comment">// Event source</span>
    <span class="field">Given</span>       <span class="type">Condition</span>[I]  <span class="comment">// Input condition</span>
    <span class="field">Then</span>        <span class="type">Action</span>[I, O]  <span class="comment">// Transform</span>
    <span class="field">GivenOutput</span> <span class="type">Condition</span>[O]  <span class="comment">// Output condition</span>
    <span class="field">To</span>          <span class="type">Destination</span>[O] <span class="comment">// Output</span>
}` },
  { name: 'Stream', code: `type <span class="type">StreamRule</span>[I, O <span class="keyword">any</span>] <span class="keyword">struct</span> {
    <span class="field">Source</span>       <span class="type">Source</span>[I]      <span class="comment">// Event source</span>
    <span class="field">Filter</span>       <span class="type">Condition</span>[I]  <span class="comment">// Input condition</span>
    <span class="field">Map</span>          <span class="type">Action</span>[I, O]  <span class="comment">// Transform</span>
    <span class="field">FilterOutput</span> <span class="type">Condition</span>[O]  <span class="comment">// Output condition</span>
    <span class="field">Sink</span>         <span class="type">Destination</span>[O] <span class="comment">// Output</span>
}` },
  { name: 'MapReduce', code: `type <span class="type">MapReduceRule</span>[I, M, Out <span class="keyword">any</span>] <span class="keyword">struct</span> {
    <span class="field">Source</span>       <span class="type">Source</span>[I]        <span class="comment">// Event source</span>
    <span class="field">Filter</span>       <span class="type">Condition</span>[I]     <span class="comment">// Input condition</span>
    <span class="field">Map</span>          <span class="type">Action</span>[I, M]     <span class="comment">// Transform</span>
    <span class="field">Reduce</span>       <span class="type">Reducer</span>[M, Out]  <span class="comment">// Accumulate</span>
    <span class="field">FilterOutput</span> <span class="type">Condition</span>[Out]   <span class="comment">// Output condition</span>
    <span class="field">Sink</span>         <span class="type">Destination</span>[Out] <span class="comment">// Output</span>
}` },
]

const activeTabCode = computed(() => {
  return tabs.find(t => t.name === activeTab.value)?.code ?? ''
})

onMounted(() => {
  const observerOptions = {
    threshold: 0.1,
    rootMargin: '0px 0px -50px 0px'
  }

  const observer = new IntersectionObserver((entries) => {
    entries.forEach(entry => {
      if (entry.isIntersecting) {
        entry.target.style.opacity = '1'
        entry.target.style.transform = 'translateY(0)'
      }
    })
  }, observerOptions)

  const animateElements = document.querySelectorAll('.feature-card, .concept-card, .use-case')
  animateElements.forEach(el => {
    el.style.opacity = '0'
    el.style.transform = 'translateY(20px)'
    el.style.transition = 'opacity 0.6s ease, transform 0.6s ease'
    observer.observe(el)
  })

  document.querySelectorAll('pre code').forEach(block => {
    const wrapper = block.parentElement
    wrapper.style.position = 'relative'

    const button = document.createElement('button')
    button.className = 'copy-button'
    button.textContent = '📋'
    button.style.cssText = `
      position: absolute;
      top: 0.5rem;
      right: 0.5rem;
      background: var(--bg-tertiary);
      border: 1px solid var(--border-color);
      border-radius: 6px;
      padding: 0.5rem;
      cursor: pointer;
      opacity: 0;
      transition: all 0.3s ease;
      font-size: 1rem;
    `

    wrapper.addEventListener('mouseenter', () => {
      button.style.opacity = '1'
    })

    wrapper.addEventListener('mouseleave', () => {
      button.style.opacity = '0'
    })

    button.addEventListener('click', () => {
      const text = block.textContent
      navigator.clipboard.writeText(text).then(() => {
        button.textContent = '✅'
        setTimeout(() => {
          button.textContent = '📋'
        }, 2000)
      })
    })

    wrapper.appendChild(button)
  })

  const handleScroll = () => {
    const scrolled = window.scrollY
    const hero = heroRef.value

    if (hero && scrolled < 800) {
      hero.style.transform = `translateY(${scrolled * 0.3}px)`
      hero.style.opacity = 1 - (scrolled / 800)
    }
  }

  window.addEventListener('scroll', handleScroll)

  return () => {
    window.removeEventListener('scroll', handleScroll)
    observer.disconnect()
  }
})
</script>

<style scoped>
.firehose-home {
  margin-top: calc(-1 * var(--vp-nav-height));
}

.hero-tabs {
  display: flex;
  gap: 0;
  margin-bottom: 1rem;
  border-bottom: 1px solid var(--border-color);
}

.hero-tab {
  background: none;
  border: none;
  border-bottom: 2px solid transparent;
  padding: 0.5rem 1rem;
  color: var(--text-secondary);
  font-family: 'SF Mono', Monaco, 'Cascadia Code', 'Roboto Mono', Consolas, monospace;
  font-size: 0.85rem;
  cursor: pointer;
  transition: all 0.2s ease;
}

.hero-tab:hover {
  color: var(--text-primary);
}

.hero-tab.active {
  color: var(--accent-primary);
  border-bottom-color: var(--accent-primary);
}
</style>
