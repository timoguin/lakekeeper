<template>
  <div>Login</div>
  <v-btn @click="login">Login</v-btn>
</template>

<script lang="ts" setup>
import { useAuth } from "../plugins/auth";
function login() {
  useAuth().signIn();
}
</script>
