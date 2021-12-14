const isCI = !!process.env.CI;

/** @type {import('@docusaurus/types').DocusaurusConfig} */
module.exports = {
  title: 'ICM Redis Operator',
  url: isCI ? 'https://operator-for-redis-cluster-docs.dev.sun.weather.com' : 'http://localhost:3001',
  baseUrl: '/',
  onBrokenLinks: 'throw',
  onBrokenMarkdownLinks: 'warn',
  favicon: 'img/favicon.ico',
  organizationName: 'IBM',
  projectName: 'operator-for-redis-cluster',
  themeConfig: {
    hideableSidebar: true,
    colorMode: {
      defaultMode: 'dark',
    },
    navbar: {
      hideOnScroll: false,
      title: 'ICM Redis Operator',
      logo: {
        src: 'images/logo.svg',
        srcDark: 'images/logo.svg',
      },
      items: [
        {
          href: 'https://operator-for-redis-cluster-docs.dev.sun.weather.com',
          label: 'GitHub',
          position: 'right',
        },
      ],
    },
    prism: {
      defaultLanguage: 'go',
      additionalLanguages: ['go'],
    },
    footer: {
      style: 'dark',
      links: [],
      copyright: `ICM Redis Operator Documentation. Built with Docusaurus.`,
    },
  },
  presets: [
    [
      '@docusaurus/preset-classic',
      {
        docs: {
          sidebarCollapsible: true,
          showLastUpdateTime: true,
          routeBasePath: '/',
          sidebarPath: './sidebars.js',
          // Please change this to your repo.
          editUrl:
            'https://operator-for-redis-cluster-docs.dev.sun.weather.com',
        },
        blog: {
          showReadingTime: true,
          // Please change this to your repo.
          editUrl:
            'https://operator-for-redis-cluster-docs.dev.sun.weather.com/blog',
        },
      },
    ],
  ],
  plugins: [
    ['@docusaurus/plugin-client-redirects', { fromExtensions: ['html', 'md'] }],
  ]
};
