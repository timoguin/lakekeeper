<template>
  <v-navigation-drawer v-model="visual.navBarShow">
    <v-list>
      <v-list-item
        link
        to="/"
        title="Home"
        prepend-icon="mdi-home"
      ></v-list-item>
      <v-list-item
        link
        to="warehouses"
        title="Warehouses"
        prepend-icon="mdi-warehouse"
      ></v-list-item>
      <v-list-item
        link
        @click="showSnackbar"
        title="Volumes"
        prepend-icon="mdi-bucket"
      ></v-list-item>
      <v-list-item
        link
        to="settings"
        title="Settings"
        prepend-icon="mdi-cog"
      ></v-list-item>
    </v-list>
    <v-snackbar v-model="snackbarVisible" timeout="3000">
      Volumes are not active yet
    </v-snackbar>
  </v-navigation-drawer>
</template>

<script setup lang="ts">
import { useVisualStore } from "../stores/visual";
const visual = useVisualStore();
const snackbarVisible = ref(false);

function showSnackbar() {
  snackbarVisible.value = true;
}
</script>
