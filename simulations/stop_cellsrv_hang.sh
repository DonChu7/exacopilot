#!/bin/bash

ip_addr=$(ip -o a show stre0 | grep -oP '(?<=inet )\S+')
ip addr add $ip_addr dev stre0