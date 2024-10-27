<template>
  <div>Name: {{ user.given_name }} {{ user.family_name }}</div>

  <div>Email: {{ user.email }}</div>
</template>

<script lang="ts" setup>
import { useUserStore } from "../stores/user";
const userStore = useUserStore();
const user = userStore.getUser();
</script>
