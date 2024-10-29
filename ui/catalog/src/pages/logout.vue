<template>
  <div>You're logged out</div>
</template>

<script lang="ts" setup>
import { useUserStore } from "../stores/user";
import { onMounted } from "vue";
import { useRouter } from "vue-router";

const userStorage = useUserStore();
const router = useRouter();

onMounted(() => {
  userStorage.isAuthenticated = false;
  userStorage.unsetUser();
  router.push("/login");
});
</script>
