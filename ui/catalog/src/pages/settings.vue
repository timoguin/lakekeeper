<template>
  <v-row>
    <v-col cols="10">
      <v-card class="ma-6">
        <v-list-item two-line class="mb-12">
          <div class="text-overline mb-4">Settings</div>
          <v-list-item-title class="text-h7 mb-1">
            Server ID: {{ projectInfo["server-id"] }}
          </v-list-item-title>
          <v-list-item-subtitle>
            <div>Server Version: {{ projectInfo.version }}</div>
            <div>Bootstrap Status: {{ projectInfo.bootstrapped }}</div>
            <div>Bootstrap Status: {{ projectInfo["authz-backend"] }}</div>
          </v-list-item-subtitle>
        </v-list-item>
      </v-card>
    </v-col>
  </v-row>
</template>

<script lang="ts" setup>
import { useVisualStore } from "@/stores/visual";
import { useFunctions } from "@/plugins/functions";

const visual = useVisualStore();
const functions = useFunctions();
onMounted(() => {
  functions.getServerInfo();
});

const projectInfo = computed(() => {
  return visual.projectInfo;
});
</script>
