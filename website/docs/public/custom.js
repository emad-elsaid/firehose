// Theme Management
const themeToggle = document.querySelector('.theme-toggle');
const html = document.documentElement;

const currentTheme = localStorage.getItem('theme') || 'dark';
html.setAttribute('data-theme', currentTheme);

themeToggle.addEventListener('click', () => {
    const theme = html.getAttribute('data-theme');
    const newTheme = theme === 'dark' ? 'light' : 'dark';

    html.setAttribute('data-theme', newTheme);
    localStorage.setItem('theme', newTheme);
});

// Smooth scroll enhancement with offset for fixed navbar
document.querySelectorAll('a[href^="#"]').forEach(anchor => {
    anchor.addEventListener('click', function (e) {
        e.preventDefault();
        const target = document.querySelector(this.getAttribute('href'));

        if (target) {
            const offsetTop = target.offsetTop - 80;
            window.scrollTo({
                top: offsetTop,
                behavior: 'smooth'
            });
        }
    });
});

// Add active state to nav links based on scroll position
window.addEventListener('scroll', () => {
    let current = '';
    const sections = document.querySelectorAll('.section');

    sections.forEach(section => {
        const sectionTop = section.offsetTop;
        if (scrollY >= (sectionTop - 150)) {
            current = section.getAttribute('id');
        }
    });

    document.querySelectorAll('.nav-links a').forEach(link => {
        link.classList.remove('active');
        if (link.getAttribute('href') === `#${current}`) {
            link.classList.add('active');
        }
    });
});

// Add code copy functionality to all pages (home page handled by HomeCustom.vue)
document.querySelectorAll('pre code').forEach(block => {
    const wrapper = block.parentElement;
    if (wrapper.querySelector('.copy-button')) return;
    wrapper.style.position = 'relative';

    const button = document.createElement('button');
    button.className = 'copy-button';
    button.textContent = '📋';
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
    `;

    wrapper.addEventListener('mouseenter', () => {
        button.style.opacity = '1';
    });

    wrapper.addEventListener('mouseleave', () => {
        button.style.opacity = '0';
    });

    button.addEventListener('click', () => {
        const text = block.textContent;
        navigator.clipboard.writeText(text).then(() => {
            button.textContent = '✅';
            setTimeout(() => {
                button.textContent = '📋';
            }, 2000);
        });
    });

    wrapper.appendChild(button);
});
