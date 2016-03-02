import packageJson from '../../package.json';
import { PluginManifest, PluginStore } from 'graylog-web-plugin/plugin';
import PipelinesOverviewPage from './pipelines/PipelinesOverviewPage';
import PipelineDetailsPage from './pipelines/PipelineDetailsPage';
import PipelinesPage from './PipelinesPage';
import RulesPage from './rules/RulesPage';

PluginStore.register(new PluginManifest(packageJson, {
  routes: [
    {path: '/system/pipelines', component: PipelinesPage},
    {path: '/system/pipelines/overview', component: PipelinesOverviewPage},
    {path: '/system/pipelines/rules', component: RulesPage},
    {path: '/system/pipelines/:pipelineId', component: PipelineDetailsPage},
  ],

  systemnavigation: [
    {path: '/system/pipelines', description: 'Pipelines'},
  ],
}));
