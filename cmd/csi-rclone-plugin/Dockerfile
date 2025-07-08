FROM alpine:3.22
RUN apk add --no-cache ca-certificates bash fuse fuse3 curl unzip tini

RUN curl https://rclone.org/install.sh | bash

COPY ./_output/csi-rclone-plugin /bin/csi-rclone-plugin

ENTRYPOINT [ "/sbin/tini", "--"]
CMD ["/bin/csi-rclone-plugin"]
