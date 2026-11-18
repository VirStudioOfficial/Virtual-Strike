@echo off
chcp 65001 >nul
title Virtual Strike LAN
where node >nul 2>nul || (echo Node.js is not installed. Get the LTS from nodejs.org & pause & exit /b)
node server.js 8080
pause
