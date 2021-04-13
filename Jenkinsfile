@Library("icm-jenkins-common@1.19.4")
import com.ibm.icm.*

icmJenkinsProperties().
  rotateBuilds(numToKeep: 30, daysToKeep: 45).
  disableConcurrentBuilds().
  apply()

ibmCloud = [
  region: 'us-south',
  apiKeyId: 'icm_bluemix_1638245'
]

docker = [
  artifactory: "wcp-icm-remote-docker-virtual.artifactory.swg-devops.com",
  registry: "us.icr.io",
  namespace: "icm-docker-images",
  operatorImageName: "icm-redis-operator",
  redisnodeImageName: "icm-redis-redisnode"
]

artifactory = [
  credentials: "TAAS-Artifactory-User-Password-Global",
  host: "na.artifactory.swg-devops.com/artifactory",
  repo: "wcp-icm-helm-local"
]

cleanupPolicy = [
  tag: [suffix: '-dev'],
  daysToKeep: 14,
  releasesToKeep: 1
]

buildImage = 'golang:1.15-buster'
releaseBranch = 'main'

podTemplate(
  inheritFrom: 'icm-jenkins-agent-helm3',
  containers: [
    containerTemplate(
      name: 'golang',
      image: "${docker.artifactory}/${buildImage}",
      ttyEnabled: true,
      resourceRequestCpu: '500m',
      resourceLimitCpu: '2',
      resourceRequestMemory: '1G',
      resourceLimitMemory: '4G'
    )
  ]
) {
  node(POD_LABEL) {

    GitInfo gitInfo = icmCheckoutStages(withTags: true)
    GitUtils gitUtils = new GitUtils(this)
    SlackNotifier slack = icmSlackNotifier('icm-redis')

    String branchSlug = StringUtils.slugify(gitInfo.branch)
    String buildDateTime = new Date().format("yyyy.MM.dd'T'HH:mm:ss", TimeZone.getTimeZone('UTC'))
    String buildCommit = gitInfo.commit
    String buildTag = gitInfo.branch

    // When building git tags gitInfo.branch contains tag name, otherwise branch name
    String imageTag = gitUtils.isTagBuild() ? gitInfo.branch : "${branchSlug}-${gitInfo.commit}-dev"
    
    // Push :latest only when building master
    boolean releaseBuild = (gitInfo.branch == releaseBranch)
    boolean isLatest = releaseBuild

    List<String> buildArgs = [
      "DOCKER_PROXY_REGISTRY=$docker.artifactory/",
      "BUILDIMAGE=$buildImage",
      "BUILDTIME=$buildDateTime",
      "TAG=$buildTag",
      "COMMIT=$buildCommit"
    ]

    stage("Run Tests") {
      try {
        container('golang') {
          sh(script: "make test")
        }
      } catch (err) {
        slack.error("icm-redis-operator build failed on stage: Run Tests. <${env.BUILD_URL}|Build logs>")
        icmMarkBuildFailed("Tests failed", true)
      }
    }

    icmDockerLogin(docker.artifactory, artifactory.credentials)
    operatorLocalImage = icmDockerBuildStage(docker.operatorImageName, imageTag, ['--build-arg': buildArgs,'--file': 'Dockerfile.operator'])
    redisnodeLocalImage = icmDockerBuildStage(docker.redisnodeImageName, imageTag, ['--build-arg': buildArgs,'--file': 'Dockerfile.redisnode'])

    if ( gitUtils.isTagBuild() || releaseBuild ) {

      icmCloudCliSetupStages(ibmCloud.apiKeyId, ibmCloud.region, CloudCliPluginConsts.CONTAINER_PLUGINS)

      DockerImageInfo operatorImageInfo = new DockerImageInfo(
        docker.registry, docker.namespace, docker.operatorImageName, imageTag, isLatest
      )
      DockerImageInfo redisnodeImageInfo = new DockerImageInfo(
        docker.registry, docker.namespace, docker.redisnodeImageName, imageTag, isLatest
      )

      icmDockerPushStage(operatorLocalImage, operatorImageInfo)
      icmDockerPushStage(redisnodeLocalImage, redisnodeImageInfo)

      icmDockerCleanupStage(image: operatorImageInfo, policy: cleanupPolicy)
      icmDockerCleanupStage(image: redisnodeImageInfo, policy: cleanupPolicy)
    }

    if ( gitUtils.isTagBuild() ) {
      HelmPackageInfo operatorHelmChart = new HelmPackageInfo(
        chartPath: "charts/icm-redis-operator",
        chartVersion: imageTag,
        appVersion: imageTag
      )
      HelmPackageInfo redisclusterHelmChart = new HelmPackageInfo(
        chartPath: "charts/icm-redis-cluster",
        chartVersion: imageTag,
        appVersion: imageTag
      )

      icmWithArtifactoryConfig(artifactory.host, artifactory.repo, artifactory.credentials) {
        icmHelmChartPackagePublishStage(operatorHelmChart, it.config.createHelmPublish())
        icmHelmChartPackagePublishStage(redisclusterHelmChart, it.config.createHelmPublish())
      }
    }

  }
}
