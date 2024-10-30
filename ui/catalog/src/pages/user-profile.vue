<template>
  <v-row>
    <v-col cols="6">
      <v-card class="ma-6">
        <v-list-item three-line class="mb-12">
          <div class="text-overline mb-4">User Profile</div>
          <v-list-item-title class="text-h5 mb-1">
            {{ user.given_name }} {{ user.family_name }}
          </v-list-item-title>
          <v-list-item-subtitle>G{{ user.email }}</v-list-item-subtitle>
        </v-list-item>
      </v-card>
    </v-col>
  </v-row>
</template>

<script lang="ts" setup>
import { useUserStore } from "../stores/user";
const userStore = useUserStore();
const user = userStore.getUser();
</script>
