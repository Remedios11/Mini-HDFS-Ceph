#include "net_channel.h"
#include "net_event_loop.h"

namespace mini_storage {

Channel::Channel(EventLoop* loop, int fd)
    : loop_(loop), fd_(fd) {}

Channel::~Channel() {
    DisableAll();
}

void Channel::EnableReading()  { events_ |= EPOLLIN;  Update(); }
void Channel::DisableReading() { events_ &= ~EPOLLIN; Update(); }
void Channel::EnableWriting()  { events_ |= EPOLLOUT;  Update(); }
void Channel::DisableWriting() { events_ &= ~EPOLLOUT; Update(); }

void Channel::DisableAll() {
    if (events_ != 0) {
        events_ = 0;
        loop_->RemoveChannel(this);
    }
}

void Channel::Update() {
    loop_->UpdateChannel(this);
}

void Channel::HandleEvent(uint32_t revents) {
    if ((revents & EPOLLHUP) && !(revents & EPOLLIN)) {
        if (close_cb_) close_cb_();
        return;
    }
    if (revents & EPOLLERR) {
        if (error_cb_) error_cb_();
        return;
    }
    if (revents & EPOLLIN) {
        if (read_cb_) read_cb_();
    }
    if (revents & EPOLLOUT) {
        if (write_cb_) write_cb_();
    }
}

}  // namespace mini_storage
