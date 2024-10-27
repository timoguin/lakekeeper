<template></template>

<script lang="ts" setup>
import { useUserStore } from "../stores/user";
import { onMounted } from "vue";
import { useRouter } from "vue-router";

const userStorage = useUserStore();
const router = useRouter();

onMounted(() => {
  userStorage.unsetUser();
  router.push("/login");
});
</script>
