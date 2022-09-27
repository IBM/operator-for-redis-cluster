const isCI = !!process.env.CI;

/** @type {import('@docusaurus/types').DocusaurusConfig} */
module.exports = {
  title: 'Operator for Redis Cluster',
  url: isCI ? 'https://ibm.github.io' : 'http://localhost:3001',
  baseUrl: '/operator-for-redis-cluster/',
  onBrokenLinks: 'throw',
  onBrokenMarkdownLinks: 'warn',
  favicon: 'img/favicon.ico',
  trailingSlash: false,
  organizationName: 'IBM',
  projectName: 'operator-for-redis-cluster',
  themeConfig: {
    docs: {
      sidebar: {
        hideable: true,
      }
    },
    colorMode: {
      defaultMode: 'dark',
    },
    navbar: {
      hideOnScroll: false,
      title: 'Operator for Redis Cluster',
      logo: {
        src: 'images/logo.svg',
        srcDark: 'images/logo.svg',
      },
      items: [
        {
          href: 'https://ibm.github.io/operator-for-redis-cluster',
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
      copyright: `Operator for Redis Cluster Documentation. Built with Docusaurus.`,
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
            'https://ibm.github.io/operator-for-redis-cluster',
        },
        blog: {
          showReadingTime: true,
          // Please change this to your repo.
          editUrl:
            'https://ibm.github.io/operator-for-redis-cluster',
        },
      },
    ],
  ],
  plugins: [
    ['@docusaurus/plugin-client-redirects', { fromExtensions: ['html', 'md'] }],
  ]
};
