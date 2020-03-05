FROM nginx:alpine

WORKDIR /var/www/client/dist/dsdp

# Copy custom nginx config
COPY nginx/nginx.conf /etc/nginx/nginx.conf

# copy client application
COPY www /var/www/client/dist/dsdp/

EXPOSE 8080

ENTRYPOINT ["nginx"]
CMD ["-g", "daemon off;"]
