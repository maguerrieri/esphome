#include "voice_assistant.h"
#include "esphome/core/defines.h"

#ifdef USE_VOICE_ASSISTANT

#include "esphome/components/socket/socket.h"
#include "esphome/core/application.h"
#include "esphome/core/log.h"

#include <cinttypes>
#include <cstdio>

#ifdef USE_VOICE_ASSISTANT_DEBUG_PRE_ROLL
#include <esp_crt_bundle.h>
#include <esp_http_client.h>
#endif

namespace esphome::voice_assistant {

static const char *const TAG = "voice_assistant";

#ifdef SAMPLE_RATE_HZ
#undef SAMPLE_RATE_HZ
#endif

static const size_t SAMPLE_RATE_HZ = 16000;

static const size_t RING_BUFFER_SAMPLES = 512 * SAMPLE_RATE_HZ / 1000;  // 512 ms * 16 kHz/ 1000 ms
static const size_t RING_BUFFER_SIZE = RING_BUFFER_SAMPLES * sizeof(int16_t);
// Local patch: with pre_roll the buffer holds the pre-roll plus everything recorded
// while Home Assistant starts the pipeline, so it's three times the stock size.
static const size_t PRE_ROLL_RING_BUFFER_SIZE = 3 * RING_BUFFER_SIZE;  // 1536 ms
static const size_t SEND_BUFFER_SAMPLES = 32 * SAMPLE_RATE_HZ / 1000;  // 32ms * 16kHz / 1000ms
static const size_t SEND_BUFFER_SIZE = SEND_BUFFER_SAMPLES * sizeof(int16_t);
static const size_t RECEIVE_SIZE = 1024;
static const size_t SPEAKER_BUFFER_SIZE = 16 * RECEIVE_SIZE;

// If one microphone channel keeps producing audio while another configured channel produces none for this
// long, treat the silent channel as failed and stop the stream. A working microphone exposes a chunk every
// SEND_BUFFER_SAMPLES (32 ms), so this is far longer than any legitimate gap between chunks.
static const uint32_t AUDIO_CHANNEL_STALL_TIMEOUT_MS = 2000;

VoiceAssistant::VoiceAssistant() { global_voice_assistant = this; }

void VoiceAssistant::setup() {
#ifdef USE_VOICE_ASSISTANT_DEBUG_PRE_ROLL
  // Local patch: buffers and a task for the debug pre-roll upload.
  if (this->debug_pre_roll_max_ms_ > 0 && this->debug_pre_roll_url_ != nullptr) {
    this->debug_pre_roll_capacity_ = this->debug_pre_roll_max_ms_ * SAMPLE_RATE_HZ / 1000 * sizeof(int16_t);
    RAMAllocator<uint8_t> allocator;
    bool ok = (this->debug_pre_roll_[0] = allocator.allocate(this->debug_pre_roll_capacity_)) != nullptr;
    if (ok && this->mic_source2_ != nullptr) {
      ok = (this->debug_pre_roll_[1] = allocator.allocate(this->debug_pre_roll_capacity_)) != nullptr;
    }
    if (ok) {
      ok = xTaskCreate(VoiceAssistant::debug_upload_task_, "va_debug_preroll", 6144, this, 1,
                       &this->debug_upload_task_handle_) == pdPASS;
    }
    if (!ok) {
      ESP_LOGE(TAG, "Not enough memory for the debug pre-roll; it's off");
      for (auto &buffer : this->debug_pre_roll_) {
        if (buffer != nullptr) {
          allocator.deallocate(buffer, this->debug_pre_roll_capacity_);
          buffer = nullptr;
        }
      }
      this->debug_upload_task_handle_ = nullptr;
      this->debug_pre_roll_max_ms_ = this->debug_pre_roll_ms_ = 0;
    }
  }
#endif
  this->mic_source_->add_data_callback([this](const std::vector<uint8_t> &data) {
    std::shared_ptr<ring_buffer::RingBuffer> temp_ring_buffer = this->ring_buffer_.lock();
    if (temp_ring_buffer != nullptr) {
      temp_ring_buffer->write((void *) data.data(), data.size());
    }
  });

  // Second microphone channel
  if (this->mic_source2_ != nullptr) {
    this->mic_source2_->add_data_callback([this](const std::vector<uint8_t> &data) {
      std::shared_ptr<ring_buffer::RingBuffer> temp_ring_buffer = this->ring_buffer2_.lock();
      if (temp_ring_buffer != nullptr) {
        temp_ring_buffer->write((void *) data.data(), data.size());
      }
    });
  }

#ifdef USE_MEDIA_PLAYER
  if (this->media_player_ != nullptr) {
    this->media_player_->add_on_state_callback([this](media_player::MediaPlayerState state) {
      switch (state) {
        case media_player::MediaPlayerState::MEDIA_PLAYER_STATE_ANNOUNCING:
          if (this->media_player_response_state_ == MediaPlayerResponseState::URL_SENT) {
            // State changed to announcing after receiving the url
            this->media_player_response_state_ = MediaPlayerResponseState::PLAYING;
          }
          break;
        default:
          if (this->media_player_response_state_ == MediaPlayerResponseState::PLAYING) {
            // No longer announcing the TTS response
            this->media_player_response_state_ = MediaPlayerResponseState::FINISHED;
          }
          break;
      }
    });
  }
#endif
}

float VoiceAssistant::get_setup_priority() const { return setup_priority::AFTER_CONNECTION; }

bool VoiceAssistant::start_udp_socket_() {
  this->socket_ = socket::socket(AF_INET, SOCK_DGRAM, IPPROTO_IP);
  if (this->socket_ == nullptr) {
    ESP_LOGE(TAG, "Could not create socket");
    this->mark_failed();
    return false;
  }
  int enable = 1;
  int err = this->socket_->setsockopt(SOL_SOCKET, SO_REUSEADDR, &enable, sizeof(int));
  if (err != 0) {
    ESP_LOGW(TAG, "Socket unable to set reuseaddr: errno %d", err);
    // we can still continue
  }
  err = this->socket_->setblocking(false);
  if (err != 0) {
    ESP_LOGE(TAG, "Socket unable to set nonblocking mode: errno %d", err);
    this->mark_failed();
    return false;
  }

#ifdef USE_SPEAKER
  if (this->speaker_ != nullptr) {
    struct sockaddr_storage server;

    socklen_t sl = socket::set_sockaddr_any((struct sockaddr *) &server, sizeof(server), 6055);
    if (sl == 0) {
      ESP_LOGE(TAG, "Socket unable to set sockaddr: errno %d", errno);
      this->mark_failed();
      return false;
    }

    err = this->socket_->bind((struct sockaddr *) &server, sizeof(server));
    if (err != 0) {
      ESP_LOGE(TAG, "Socket unable to bind: errno %d", errno);
      this->mark_failed();
      return false;
    }
  }
#endif
  this->udp_socket_running_ = true;
  return true;
}

bool VoiceAssistant::allocate_buffers_() {
#ifdef USE_SPEAKER
  if ((this->speaker_ != nullptr) && (this->speaker_buffer_ == nullptr)) {
    RAMAllocator<uint8_t> speaker_allocator;
    this->speaker_buffer_ = speaker_allocator.allocate(SPEAKER_BUFFER_SIZE);
    if (this->speaker_buffer_ == nullptr) {
      ESP_LOGW(TAG, "Could not allocate speaker buffer");
      return false;
    }
  }
#endif

  if (this->audio_source_ == nullptr) {
    std::shared_ptr<ring_buffer::RingBuffer> temp_ring_buffer = ring_buffer::RingBuffer::create(this->ring_buffer_size_());
    if (temp_ring_buffer == nullptr) {
      ESP_LOGE(TAG, "Could not allocate ring buffer");
      return false;
    }
    // Zero-copy source that reads directly from the ring buffer; frame-aligned to never split an int16 sample.
    this->audio_source_ = audio::RingBufferAudioSource::create(temp_ring_buffer, SEND_BUFFER_SIZE, sizeof(int16_t));
    if (this->audio_source_ == nullptr) {
      ESP_LOGE(TAG, "Could not allocate audio source");
      return false;
    }
    this->ring_buffer_ = temp_ring_buffer;
  }

  // Second microphone channel
  if ((this->mic_source2_ != nullptr) && (this->audio_source2_ == nullptr)) {
    std::shared_ptr<ring_buffer::RingBuffer> temp_ring_buffer = ring_buffer::RingBuffer::create(this->ring_buffer_size_());
    if (temp_ring_buffer == nullptr) {
      ESP_LOGE(TAG, "Could not allocate second ring buffer");
      return false;
    }
    this->audio_source2_ = audio::RingBufferAudioSource::create(temp_ring_buffer, SEND_BUFFER_SIZE, sizeof(int16_t));
    if (this->audio_source2_ == nullptr) {
      ESP_LOGE(TAG, "Could not allocate second audio source");
      return false;
    }
    this->ring_buffer2_ = temp_ring_buffer;
  }

  return true;
}

size_t VoiceAssistant::ring_buffer_size_() const {
  // Local patch: with a debug pre-roll the buffer also holds it at the moment of a
  // wake-word start, when it's copied out, before the pipeline start round trip.
  // Once older audio is dropped, the extra 512 ms keeps the microphone from
  // overwriting the capture while it's read.
  if (this->debug_pre_roll_max_ms_ > 0) {
    size_t capture = static_cast<size_t>(this->pre_roll_ms_ + this->debug_pre_roll_max_ms_) * SAMPLE_RATE_HZ / 1000 *
                     sizeof(int16_t);
    return std::max(PRE_ROLL_RING_BUFFER_SIZE, capture + RING_BUFFER_SIZE);
  }
  return this->pre_roll_ms_ > 0 ? PRE_ROLL_RING_BUFFER_SIZE : RING_BUFFER_SIZE;
}

void VoiceAssistant::clear_buffers_(size_t keep_bytes) {
  if (keep_bytes == 0) {
    if (this->audio_source_ != nullptr) {
      this->audio_source_->clear_buffered_data();
    }

    // Second microphone channel
    if (this->audio_source2_ != nullptr) {
      this->audio_source2_->clear_buffered_data();
    }
  } else {
    // Local patch: drop all but the newest keep_bytes. Nothing reads the ring
    // buffers while idle, and update_pre_roll_() cleared them (releasing any item
    // a source still held) when the microphone restarted, so plain reads are safe.
    uint8_t scratch[1024];
    for (const auto &weak : {this->ring_buffer_, this->ring_buffer2_}) {
      std::shared_ptr<ring_buffer::RingBuffer> rb = weak.lock();
      if (rb == nullptr) {
        continue;
      }
      size_t available = rb->available();
      while (available > keep_bytes) {
        size_t chunk = std::min(sizeof(scratch), available - keep_bytes);
        chunk -= chunk % sizeof(int16_t);
        if (chunk == 0 || rb->read(scratch, chunk) == 0) {
          break;
        }
        available = rb->available();
      }
    }
  }

  // Reset the multi-channel stall watchdog (see audio_channel_stall_start_).
  this->audio_channel_stall_start_ = 0;

#ifdef USE_SPEAKER
  if ((this->speaker_ != nullptr) && (this->speaker_buffer_ != nullptr)) {
    memset(this->speaker_buffer_, 0, SPEAKER_BUFFER_SIZE);

    this->speaker_buffer_size_ = 0;
    this->speaker_buffer_index_ = 0;
    this->speaker_bytes_received_ = 0;
  }
#endif
}

// Local patch: while idle, keep the microphone sources and ring buffers running
// so they always hold the latest audio (RingBuffer::write drops the oldest bytes
// when full), but only while the on-device wake word is listening and Home
// Assistant is connected, the only times a wake-word start can happen.
void VoiceAssistant::update_pre_roll_() {
#ifdef USE_MICRO_WAKE_WORD
  bool wanted =
      this->api_client_ != nullptr && this->micro_wake_word_ != nullptr && this->micro_wake_word_->is_running();
#else
  bool wanted = false;  // no on-device wake word, so no wake-word start to pre-roll for
#endif
  bool running = this->mic_source_->is_running() || (this->mic_source2_ && this->mic_source2_->is_running());
  if (!wanted) {
    if (running) {
      this->mic_source_->stop();
      if (this->mic_source2_) {
        this->mic_source2_->stop();
      }
    }
    this->deallocate_buffers_();
    return;
  }
  if (running || this->pre_roll_alloc_failed_) {
    return;
  }
  if (!this->allocate_buffers_()) {
    // Don't retry every loop; the next pipeline start allocates (and logs) again.
    this->pre_roll_alloc_failed_ = true;
    return;
  }
  // Fresh start: drop audio and any item a source still holds from the last request.
  this->clear_buffers_();
  this->mic_source_->start();
  if (this->mic_source2_) {
    this->mic_source2_->start();
  }
}

#ifdef USE_VOICE_ASSISTANT_DEBUG_PRE_ROLL
// Local patch: copy the audio from just before the kept pre-roll into the debug
// buffers, for the upload task to send when the request is over. Reads continue
// from the front, so the capture ends where the kept pre-roll starts, give or
// take what the microphone added meanwhile, which clear_buffers_(keep_bytes) drops.
void VoiceAssistant::capture_debug_pre_roll_(size_t keep_bytes) {
  if (this->debug_pre_roll_ms_ == 0 || this->debug_upload_task_handle_ == nullptr) {
    return;
  }
  if (this->debug_upload_busy_.load()) {
    ESP_LOGW(TAG, "Debug pre-roll upload still running; not capturing this one");
    return;
  }
  std::shared_ptr<ring_buffer::RingBuffer> rbs[2] = {this->ring_buffer_.lock(), this->ring_buffer2_.lock()};
  const size_t channels = (rbs[1] != nullptr && this->debug_pre_roll_[1] != nullptr) ? 2 : 1;
  if (rbs[0] == nullptr) {
    return;
  }
  size_t available = rbs[0]->available();
  if (channels == 2) {
    available = std::min(available, rbs[1]->available());
  }
  if (available <= keep_bytes) {
    return;
  }
  size_t len = std::min<size_t>(this->debug_pre_roll_ms_ * SAMPLE_RATE_HZ / 1000 * sizeof(int16_t), available - keep_bytes);
  len = std::min(len, this->debug_pre_roll_capacity_);
  len -= len % sizeof(int16_t);
  // Small reads, so the microphone task is never held off for long.
  static const size_t CHUNK = 1024;
  for (size_t c = 0; c < channels; c++) {
    uint8_t *buffer = this->debug_pre_roll_[c];
    // Drop anything older than the capture, using the buffer as scratch space and
    // re-checking as the microphone adds audio, so the capture ends where the kept
    // pre-roll starts. A short wait rides out the microphone task's own reads.
    size_t available_now;
    while ((available_now = rbs[c]->available()) > keep_bytes + len) {
      if (rbs[c]->read(buffer, std::min(available_now - keep_bytes - len, CHUNK), pdMS_TO_TICKS(10)) == 0) {
        break;
      }
    }
    size_t got = 0;
    while (got < len) {
      size_t n = rbs[c]->read(buffer + got, std::min(len - got, CHUNK), pdMS_TO_TICKS(10));
      if (n == 0) {
        break;
      }
      got += n;
    }
    if (got < len) {
      // Keeping a short channel would leave the channels covering different times.
      ESP_LOGW(TAG, "Debug pre-roll capture came up short; skipping it");
      return;
    }
  }
  this->debug_pre_roll_len_ = len;
  this->debug_pre_roll_channels_ = channels;
  this->debug_pre_roll_captured_at_ = millis();
  this->debug_pre_roll_pending_ = true;
  this->debug_upload_busy_.store(true);
}

void VoiceAssistant::debug_upload_task_(void *arg) {
  auto *va = static_cast<VoiceAssistant *>(arg);
  while (true) {
    ulTaskNotifyTake(pdTRUE, portMAX_DELAY);
    va->upload_debug_pre_roll_();
    va->debug_upload_busy_.store(false);
  }
}

static bool http_write_all(esp_http_client_handle_t client, const void *data, size_t len) {
  const char *p = static_cast<const char *>(data);
  while (len > 0) {
    int n = esp_http_client_write(client, p, len);
    if (n <= 0) {
      return false;
    }
    p += n;
    len -= n;
  }
  return true;
}

// Runs on the upload task. Sends the capture as a 16 kHz 16-bit WAV, one channel
// per microphone channel, interleaved in small chunks. X-Capture-Age-Ms lets Home
// Assistant find the pipeline run the capture belongs to. The URL carries the
// webhook id, so it's never logged.
void VoiceAssistant::upload_debug_pre_roll_() {
  const uint32_t channels = this->debug_pre_roll_channels_;
  const uint32_t samples = this->debug_pre_roll_len_ / sizeof(int16_t);
  const uint32_t data_bytes = samples * channels * sizeof(int16_t);
  uint8_t header[44];
  auto put = [&header](size_t at, uint32_t value, size_t bytes) {
    for (size_t i = 0; i < bytes; i++) {
      header[at + i] = (value >> (8 * i)) & 0xff;
    }
  };
  memcpy(header, "RIFF", 4);
  put(4, 36 + data_bytes, 4);
  memcpy(header + 8, "WAVEfmt ", 8);
  put(16, 16, 4);
  put(20, 1, 2);  // PCM
  put(22, channels, 2);
  put(24, SAMPLE_RATE_HZ, 4);
  put(28, SAMPLE_RATE_HZ * channels * sizeof(int16_t), 4);
  put(32, channels * sizeof(int16_t), 2);
  put(34, 16, 2);
  memcpy(header + 36, "data", 4);
  put(40, data_bytes, 4);

  esp_http_client_config_t config = {};
  config.url = this->debug_pre_roll_url_;
  config.method = HTTP_METHOD_POST;
  config.timeout_ms = 10000;
  config.crt_bundle_attach = esp_crt_bundle_attach;  // for an https:// URL
  esp_http_client_handle_t client = esp_http_client_init(&config);
  if (client == nullptr) {
    ESP_LOGW(TAG, "Debug pre-roll upload: couldn't create an HTTP client");
    return;
  }
  esp_http_client_set_header(client, "Content-Type", "audio/wav");
  char age[12];
  snprintf(age, sizeof(age), "%" PRIu32, static_cast<uint32_t>(millis() - this->debug_pre_roll_captured_at_));
  esp_http_client_set_header(client, "X-Capture-Age-Ms", age);
  // Lets Home Assistant look only at this device's runs.
  esp_http_client_set_header(client, "X-Device-MAC", get_mac_address().c_str());
  const char *failure = nullptr;
  esp_err_t err = esp_http_client_open(client, sizeof(header) + data_bytes);
  if (err != ESP_OK) {
    failure = esp_err_to_name(err);
  } else if (!http_write_all(client, header, sizeof(header))) {
    failure = "write failed";
  }
  bool ok = failure == nullptr;
  const int16_t *in[2] = {reinterpret_cast<const int16_t *>(this->debug_pre_roll_[0]),
                          reinterpret_cast<const int16_t *>(this->debug_pre_roll_[1])};
  int16_t out[256];
  for (uint32_t i = 0; ok && i < samples;) {
    uint32_t n = std::min<uint32_t>(256 / channels, samples - i);
    for (uint32_t j = 0; j < n; j++) {
      for (uint32_t c = 0; c < channels; c++) {
        out[j * channels + c] = in[c][i + j];
      }
    }
    ok = http_write_all(client, out, n * channels * sizeof(int16_t));
    if (!ok) {
      failure = "write failed";
    }
    i += n;
  }
  int status = -1;
  if (ok) {
    if (esp_http_client_fetch_headers(client) < 0) {
      failure = "no response";
    } else if ((status = esp_http_client_get_status_code(client)) < 200 || status >= 300) {
      failure = "rejected";
    }
  }
  if (failure == nullptr) {
    ESP_LOGD(TAG, "Uploaded %" PRIu32 " ms of debug pre-roll", static_cast<uint32_t>(samples * 1000 / SAMPLE_RATE_HZ));
  } else {
    ESP_LOGW(TAG, "Debug pre-roll upload failed: %s (HTTP status %d)", failure, status);
  }
  esp_http_client_close(client);
  esp_http_client_cleanup(client);
}
#endif

void VoiceAssistant::deallocate_buffers_() {
  // Destroying each source releases its ring buffer; the matching weak_ptr then expires automatically.
  this->audio_source_.reset();

  // Second microphone channel
  this->audio_source2_.reset();

#ifdef USE_SPEAKER
  if ((this->speaker_ != nullptr) && (this->speaker_buffer_ != nullptr)) {
    RAMAllocator<uint8_t> speaker_deallocator;
    speaker_deallocator.deallocate(this->speaker_buffer_, SPEAKER_BUFFER_SIZE);
    this->speaker_buffer_ = nullptr;
  }
#endif
}

void VoiceAssistant::reset_conversation_id() {
  this->conversation_id_ = "";
  ESP_LOGD(TAG, "reset conversation ID");
}

void VoiceAssistant::stream_api_audio_() {
  // Both microphone channels are sent together, if configured. Home Assistant feeds one of the
  // channels to its speech-to-text stream and treats an empty payload on that channel as
  // end-of-stream, and the device cannot know which channel it picked, so only send once every
  // configured channel has audio exposed, and always send them together. We don't target any
  // particular message size: Home Assistant re-chunks the audio, and each fill() exposes at most
  // SEND_BUFFER_SIZE bytes.
  while (true) {
    // fill() exposes a new chunk, or returns 0 if a previous chunk is still exposed; available()
    // reports the currently exposed bytes either way.
    this->audio_source_->fill(0, false);
    size_t available = this->audio_source_->available();
    size_t available2 = 0;
    if (this->audio_source2_ != nullptr) {
      this->audio_source2_->fill(0, false);
      available2 = this->audio_source2_->available();
    }

    const bool channel_empty = (available == 0);
    const bool channel2_empty = (this->audio_source2_ != nullptr) && (available2 == 0);
    if (channel_empty || channel2_empty) {
      // A configured channel has no audio yet, so keep any chunk exposed on the other channel for the
      // next pass rather than sending an empty payload.
      this->handle_channel_stall_(available, available2);
      break;
    }

    // Both channels have audio exposed; clear any in-progress stall timer.
    this->audio_channel_stall_start_ = 0;

    api::VoiceAssistantAudio msg;
    // Zero-copy: send_message() copies the data out before we consume it.
    msg.data = this->audio_source_->data();
    msg.data_len = available;
    if (this->audio_source2_ != nullptr) {
      msg.data2 = this->audio_source2_->data();
      msg.data2_len = available2;
    }

    if (!this->api_client_->send_message(msg)) {
      // Keep the chunk exposed and retry next pass, the same shape as
      // APIConnection::try_send_camera_image_(): the slice is only lost if
      // the ring buffer overflows before the TCP buffer clears, instead of
      // on every refusal. The api layer already reports the refusal at V.
      return;
    }

    this->audio_source_->consume(available);
    if (this->audio_source2_ != nullptr) {
      this->audio_source2_->consume(available2);
    }
  }
}

void VoiceAssistant::handle_channel_stall_(size_t available, size_t available2) {
  // Called when at least one configured channel has no audio exposed. When one channel has data and the
  // other does not, watch how long the empty channel stays starved: Home Assistant has no stream timeout
  // and would never tell us to stop, so a channel that fails outright would otherwise hang streaming
  // forever with the live channel's chunk held. Stop the stream with an error after a prolonged imbalance.
  if ((available == 0) && (available2 == 0)) {
    // Both channels are idle (no audio buffered yet); normal, not a stalled channel.
    this->audio_channel_stall_start_ = 0;
    return;
  }

  const uint32_t now = App.get_loop_component_start_time();
  if (this->audio_channel_stall_start_ == 0) {
    this->audio_channel_stall_start_ = now;
  } else if ((now - this->audio_channel_stall_start_) >= AUDIO_CHANNEL_STALL_TIMEOUT_MS) {
    ESP_LOGW(TAG, "Mic channel %d stalled, stopping stream", (available == 0) ? 0 : 1);
    this->audio_channel_stall_start_ = 0;
    this->signal_stop_();
    this->set_state_(State::STOP_MICROPHONE, State::IDLE);
    this->defer([this]() {
      this->error_trigger_.trigger("mic-channel-stalled", "A microphone channel stopped producing audio");
    });
  }
}

void VoiceAssistant::loop() {
  if (this->api_client_ == nullptr && this->state_ != State::IDLE && this->state_ != State::STOP_MICROPHONE &&
      this->state_ != State::STOPPING_MICROPHONE) {
    if (this->mic_source_->is_running() || (this->mic_source2_ && this->mic_source2_->is_running()) ||
        this->state_ == State::STARTING_MICROPHONE) {
      this->set_state_(State::STOP_MICROPHONE, State::IDLE);
    } else {
      this->set_state_(State::IDLE, State::IDLE);
    }
    this->continuous_ = false;
    this->signal_stop_();
    this->clear_buffers_();
    return;
  }
  switch (this->state_) {
    case State::IDLE: {
#ifdef USE_VOICE_ASSISTANT_DEBUG_PRE_ROLL
      // Local patch: upload a debug pre-roll once its request is over, so it
      // doesn't compete with the voice stream.
      if (this->debug_pre_roll_pending_) {
        this->debug_pre_roll_pending_ = false;
        xTaskNotifyGive(this->debug_upload_task_handle_);
      }
#endif
      if (this->continuous_ && this->desired_state_ == State::IDLE) {
        this->idle_trigger_.trigger();
        this->set_state_(State::START_MICROPHONE, State::START_PIPELINE);
      } else if (this->pre_roll_ms_ > 0) {
        this->update_pre_roll_();
      } else {
        this->deallocate_buffers_();
      }
      break;
    }
    case State::START_MICROPHONE: {
      ESP_LOGD(TAG, "Starting Microphone");
      if (!this->allocate_buffers_()) {
        this->status_set_error(LOG_STR("Failed to allocate buffers"));
        return;
      }
      if (this->status_has_error()) {
        this->status_clear_error();
      }
      this->pre_roll_alloc_failed_ = false;
      // Local patch: keep the pre-roll only for a wake-word start. A button press,
      // a Home Assistant-initiated start or a follow-up turn starts clean, as stock.
      if (this->pre_roll_ms_ > 0 && !this->wake_word_.empty() && !this->continue_conversation_) {
        size_t keep_bytes = this->pre_roll_ms_ * SAMPLE_RATE_HZ / 1000 * sizeof(int16_t);
#ifdef USE_VOICE_ASSISTANT_DEBUG_PRE_ROLL
        this->capture_debug_pre_roll_(keep_bytes);
#endif
        this->clear_buffers_(keep_bytes);
      } else {
        this->clear_buffers_();
      }

      this->mic_source_->start();
      if (this->mic_source2_) {
        this->mic_source2_->start();
      }
      this->set_state_(State::STARTING_MICROPHONE);
      break;
    }
    case State::STARTING_MICROPHONE: {
      if (this->mic_source_->is_running() && (!this->mic_source2_ || this->mic_source2_->is_running())) {
        this->set_state_(this->desired_state_);
      }
      break;
    }
    case State::START_PIPELINE: {
      ESP_LOGD(TAG, "Requesting start");
      uint32_t flags = 0;
      if (!this->continue_conversation_ && this->use_wake_word_)
        flags |= api::enums::VOICE_ASSISTANT_REQUEST_USE_WAKE_WORD;
      if (this->silence_detection_)
        flags |= api::enums::VOICE_ASSISTANT_REQUEST_USE_VAD;
      api::VoiceAssistantAudioSettings audio_settings;
      audio_settings.noise_suppression_level = this->noise_suppression_level_;
      audio_settings.auto_gain = this->auto_gain_;
      audio_settings.volume_multiplier = this->volume_multiplier_;

      api::VoiceAssistantRequest msg;
      msg.start = true;
      msg.conversation_id = StringRef(this->conversation_id_);
      msg.flags = flags;
      msg.audio_settings = audio_settings;
      msg.wake_word_phrase = StringRef(this->wake_word_);

      // Reset media player state tracking
#ifdef USE_MEDIA_PLAYER
      if (this->media_player_ != nullptr) {
        this->media_player_response_state_ = MediaPlayerResponseState::IDLE;
      }
#endif

      if (this->api_client_ == nullptr || !this->api_client_->send_message(msg)) {
        ESP_LOGW(TAG, "Could not request start");
        this->error_trigger_.trigger("not-connected", "Could not request start");
        this->continuous_ = false;
        this->set_state_(State::IDLE, State::IDLE);
        break;
      }
      this->set_state_(State::STARTING_PIPELINE);
      this->set_timeout("reset-conversation_id", this->conversation_timeout_,
                        [this]() { this->reset_conversation_id(); });
      break;
    }
    case State::STARTING_PIPELINE: {
      break;  // State changed when udp server port received
    }
    case State::STREAMING_MICROPHONE: {
      // pre_shift is ignored by RingBufferAudioSource (no intermediate transfer buffer to compact).
      if (this->audio_mode_ == AUDIO_MODE_API) {
        this->stream_api_audio_();
      } else {
        // UDP (will eventually be deprecated)
        // Only the primary microphone channel is used
        while (true) {
          this->audio_source_->fill(0, false);
          size_t available = this->audio_source_->available();
          if (available == 0) {
            break;
          }
          if (!this->udp_socket_running_) {
            if (!this->start_udp_socket_()) {
              this->set_state_(State::STOP_MICROPHONE, State::IDLE);
              break;
            }
          }
          this->socket_->sendto(this->audio_source_->data(), available, 0, (struct sockaddr *) &this->dest_addr_,
                                sizeof(this->dest_addr_));
          this->audio_source_->consume(available);
        }
      }  // audio mode
      break;
    }
    case State::STOP_MICROPHONE: {
      // Check both microphone channels
      bool is_running = this->mic_source_->is_running();
      bool is_running2 = false;
      if (this->mic_source2_) {
        is_running2 = this->mic_source2_->is_running();
      }
      if (is_running || is_running2) {
        if (is_running) {
          this->mic_source_->stop();
        }
        if (is_running2) {
          this->mic_source2_->stop();
        }
        this->set_state_(State::STOPPING_MICROPHONE);
      } else {
        this->set_state_(this->desired_state_);
      }
      break;
    }
    case State::STOPPING_MICROPHONE: {
      // Check both microphone channels
      bool is_stopped = this->mic_source_->is_stopped();
      bool is_stopped2 = true;
      if (this->mic_source2_) {
        is_stopped2 = this->mic_source2_->is_stopped();
      }
      if (is_stopped && is_stopped2) {
        this->set_state_(this->desired_state_);
      }
      break;
    }
    case State::AWAITING_RESPONSE: {
      break;  // State changed by events
    }
    case State::STREAMING_RESPONSE: {
      bool playing = false;
#ifdef USE_SPEAKER
      if (this->speaker_ != nullptr) {
        ssize_t received_len = 0;
        if (this->audio_mode_ == AUDIO_MODE_UDP) {
          if (this->speaker_buffer_index_ + RECEIVE_SIZE < SPEAKER_BUFFER_SIZE) {
            received_len = this->socket_->read(this->speaker_buffer_ + this->speaker_buffer_index_, RECEIVE_SIZE);
            if (received_len > 0) {
              this->speaker_buffer_index_ += received_len;
              this->speaker_buffer_size_ += received_len;
              this->speaker_bytes_received_ += received_len;
            }
          } else {
            ESP_LOGD(TAG, "Receive buffer full");
          }
        }
        // Build a small buffer of audio before sending to the speaker
        bool end_of_stream = this->stream_ended_ && (this->audio_mode_ == AUDIO_MODE_API || received_len < 0);
        if (this->speaker_bytes_received_ > RECEIVE_SIZE * 4 || end_of_stream)
          this->write_speaker_();
        if (this->wait_for_stream_end_) {
          this->cancel_timeout("playing");
          if (end_of_stream) {
            ESP_LOGD(TAG, "End of audio stream received");
            this->cancel_timeout("speaker-timeout");
            this->set_state_(State::RESPONSE_FINISHED, State::RESPONSE_FINISHED);
          }
          break;  // We dont want to timeout here as the STREAM_END event will take care of that.
        }
        playing = this->speaker_->is_running();
      }
#endif
#ifdef USE_MEDIA_PLAYER
      if (this->media_player_ != nullptr) {
        playing = (this->media_player_response_state_ == MediaPlayerResponseState::PLAYING);

        if (this->media_player_response_state_ == MediaPlayerResponseState::FINISHED) {
          this->media_player_response_state_ = MediaPlayerResponseState::IDLE;
          this->cancel_timeout("playing");
          ESP_LOGD(TAG, "Announcement finished playing");
          this->set_state_(State::RESPONSE_FINISHED, State::RESPONSE_FINISHED);

          api::VoiceAssistantAnnounceFinished msg;
          msg.success = true;
          if (!this->api_client_->send_message(msg)) {
            API_LOG_MSG_DROPPED(TAG, "Announce-finished");
          }
          break;
        }
      }
#endif
      if (playing) {
        this->start_playback_timeout_();
      }
      break;
    }
    case State::RESPONSE_FINISHED: {
#ifdef USE_SPEAKER
      if (this->speaker_ != nullptr) {
        if (this->speaker_buffer_size_ > 0) {
          this->write_speaker_();
          break;
        }
        if (this->speaker_->has_buffered_data() || this->speaker_->is_running()) {
          break;
        }
        ESP_LOGD(TAG, "Speaker has finished outputting all audio");
        this->speaker_->stop();
        this->cancel_timeout("speaker-timeout");
        this->cancel_timeout("playing");

        this->clear_buffers_();

        this->wait_for_stream_end_ = false;
        this->stream_ended_ = false;

        this->tts_stream_end_trigger_.trigger();
      }
#endif
      if (this->continue_conversation_) {
        this->set_state_(State::START_MICROPHONE, State::START_PIPELINE);
      } else {
        this->set_state_(State::IDLE, State::IDLE);
      }
      break;
    }
    default:
      break;
  }
}

#ifdef USE_SPEAKER
void VoiceAssistant::write_speaker_() {
  if ((this->speaker_ != nullptr) && (this->speaker_buffer_ != nullptr)) {
    if (this->speaker_buffer_size_ > 0) {
      size_t write_chunk = std::min<size_t>(this->speaker_buffer_size_, 4 * 1024);
      size_t written = this->speaker_->play(this->speaker_buffer_, write_chunk);
      if (written > 0) {
        memmove(this->speaker_buffer_, this->speaker_buffer_ + written, this->speaker_buffer_size_ - written);
        this->speaker_buffer_size_ -= written;
        this->speaker_buffer_index_ -= written;
        this->set_timeout("speaker-timeout", 5000, [this]() { this->speaker_->stop(); });
      } else {
        ESP_LOGV(TAG, "Speaker buffer full, trying again next loop");
      }
    }
  }
}
#endif

void VoiceAssistant::client_subscription(api::APIConnection *client, bool subscribe) {
  if (!subscribe) {
    if (this->api_client_ == nullptr || client != this->api_client_) {
      ESP_LOGE(TAG, "Client attempting to unsubscribe that is not the current API Client");
      return;
    }
    this->api_client_ = nullptr;
    this->client_disconnected_trigger_.trigger();
    return;
  }

  if (this->api_client_ != nullptr) {
    char current_peername[socket::SOCKADDR_STR_LEN];
    char new_peername[socket::SOCKADDR_STR_LEN];
    ESP_LOGE(TAG,
             "Multiple API Clients attempting to connect to Voice Assistant\n"
             "  Current client: %s (%s)\n"
             "  New client: %s (%s)",
             this->api_client_->get_name(), this->api_client_->get_peername_to(current_peername), client->get_name(),
             client->get_peername_to(new_peername));
    return;
  }

  this->api_client_ = client;
  this->client_connected_trigger_.trigger();
}

static const LogString *voice_assistant_state_to_string(State state) {
  switch (state) {
    case State::IDLE:
      return LOG_STR("IDLE");
    case State::START_MICROPHONE:
      return LOG_STR("START_MICROPHONE");
    case State::STARTING_MICROPHONE:
      return LOG_STR("STARTING_MICROPHONE");
    case State::WAIT_FOR_VAD:
      return LOG_STR("WAIT_FOR_VAD");
    case State::WAITING_FOR_VAD:
      return LOG_STR("WAITING_FOR_VAD");
    case State::START_PIPELINE:
      return LOG_STR("START_PIPELINE");
    case State::STARTING_PIPELINE:
      return LOG_STR("STARTING_PIPELINE");
    case State::STREAMING_MICROPHONE:
      return LOG_STR("STREAMING_MICROPHONE");
    case State::STOP_MICROPHONE:
      return LOG_STR("STOP_MICROPHONE");
    case State::STOPPING_MICROPHONE:
      return LOG_STR("STOPPING_MICROPHONE");
    case State::AWAITING_RESPONSE:
      return LOG_STR("AWAITING_RESPONSE");
    case State::STREAMING_RESPONSE:
      return LOG_STR("STREAMING_RESPONSE");
    case State::RESPONSE_FINISHED:
      return LOG_STR("RESPONSE_FINISHED");
    default:
      return LOG_STR("UNKNOWN");
  }
};

void VoiceAssistant::set_state_(State state) {
  State old_state = this->state_;
  this->state_ = state;
  ESP_LOGD(TAG, "State changed from %s to %s", LOG_STR_ARG(voice_assistant_state_to_string(old_state)),
           LOG_STR_ARG(voice_assistant_state_to_string(state)));
}

void VoiceAssistant::set_state_(State state, State desired_state) {
  this->set_state_(state);
  this->desired_state_ = desired_state;
  ESP_LOGD(TAG, "Desired state set to %s", LOG_STR_ARG(voice_assistant_state_to_string(desired_state)));
}

void VoiceAssistant::failed_to_start() {
  ESP_LOGE(TAG, "Failed to start server. See Home Assistant logs for more details.");
  this->error_trigger_.trigger("failed-to-start", "Failed to start server. See Home Assistant logs for more details.");
  this->set_state_(State::STOP_MICROPHONE, State::IDLE);
}

void VoiceAssistant::start_streaming() {
  if (this->state_ != State::STARTING_PIPELINE) {
    this->signal_stop_();
    return;
  }

  ESP_LOGD(TAG, "Client started, streaming microphone");
  this->audio_mode_ = AUDIO_MODE_API;

  // Both microphone channels
  if (this->mic_source_->is_running() && (!this->mic_source2_ || this->mic_source2_->is_running())) {
    this->set_state_(State::STREAMING_MICROPHONE, State::STREAMING_MICROPHONE);
  } else {
    this->set_state_(State::START_MICROPHONE, State::STREAMING_MICROPHONE);
  }
}

void VoiceAssistant::start_streaming(struct sockaddr_storage *addr, uint16_t port) {
  if (this->state_ != State::STARTING_PIPELINE) {
    this->signal_stop_();
    return;
  }

  ESP_LOGD(TAG, "Client started, streaming microphone");
  this->audio_mode_ = AUDIO_MODE_UDP;

  if (this->mic_source2_ != nullptr) {
    ESP_LOGW(TAG, "UDP audio mode does not support a second microphone channel; only the primary will be streamed");
  }

  memcpy(&this->dest_addr_, addr, sizeof(this->dest_addr_));
  if (this->dest_addr_.ss_family == AF_INET) {
    ((struct sockaddr_in *) &this->dest_addr_)->sin_port = htons(port);
  }
#if LWIP_IPV6
  else if (this->dest_addr_.ss_family == AF_INET6) {
    ((struct sockaddr_in6 *) &this->dest_addr_)->sin6_port = htons(port);
  }
#endif
  else {
    ESP_LOGW(TAG, "Unknown address family: %d", this->dest_addr_.ss_family);
    return;
  }

  // Only primary microphone channel over UDP
  if (this->mic_source_->is_running()) {
    this->set_state_(State::STREAMING_MICROPHONE, State::STREAMING_MICROPHONE);
  } else {
    this->set_state_(State::START_MICROPHONE, State::STREAMING_MICROPHONE);
  }
}

void VoiceAssistant::request_start(bool continuous, bool silence_detection) {
  if (this->api_client_ == nullptr) {
    ESP_LOGE(TAG, "No API client connected");
    this->set_state_(State::IDLE, State::IDLE);
    this->continuous_ = false;
    return;
  }
  if (this->state_ == State::IDLE) {
    this->continuous_ = continuous;
    this->silence_detection_ = silence_detection;

    this->set_state_(State::START_MICROPHONE, State::START_PIPELINE);
  }
}

void VoiceAssistant::request_stop() {
  this->continuous_ = false;
  this->continue_conversation_ = false;

  switch (this->state_) {
    case State::IDLE:
      break;
    case State::START_MICROPHONE:
    case State::STARTING_MICROPHONE:
    case State::WAIT_FOR_VAD:
    case State::WAITING_FOR_VAD:
    case State::START_PIPELINE:
      this->set_state_(State::STOP_MICROPHONE, State::IDLE);
      break;
    case State::STARTING_PIPELINE:
    case State::STREAMING_MICROPHONE:
      this->signal_stop_();
      this->set_state_(State::STOP_MICROPHONE, State::IDLE);
      break;
    case State::STOP_MICROPHONE:
    case State::STOPPING_MICROPHONE:
      this->desired_state_ = State::IDLE;
      break;
    case State::AWAITING_RESPONSE:
      this->signal_stop_();
      break;
    case State::STREAMING_RESPONSE:
#ifdef USE_MEDIA_PLAYER
      // Stop any ongoing media player announcement
      if (this->media_player_ != nullptr) {
        this->media_player_->make_call()
            .set_command(media_player::MEDIA_PLAYER_COMMAND_STOP)
            .set_announcement(true)
            .perform();
      }
      if (this->started_streaming_tts_) {
        // Haven't reached the TTS_END stage, so send the stop signal to HA.
        this->signal_stop_();
      }
#endif
      break;
    case State::RESPONSE_FINISHED:
      break;  // Let the incoming audio stream finish then it will go to idle.
  }
}

void VoiceAssistant::signal_stop_() {
  memset(&this->dest_addr_, 0, sizeof(this->dest_addr_));
  if (this->api_client_ == nullptr) {
    return;
  }
  ESP_LOGD(TAG, "Signaling stop");
  api::VoiceAssistantRequest msg;
  msg.start = false;
  if (!this->api_client_->send_message(msg)) {
    API_LOG_MSG_DROPPED(TAG, "Stop request");
  }
}

void VoiceAssistant::start_playback_timeout_() {
  this->set_timeout("playing", 2000, [this]() {
    this->cancel_timeout("speaker-timeout");
    this->set_state_(State::RESPONSE_FINISHED, State::RESPONSE_FINISHED);

    if (this->api_client_ == nullptr)
      return;
    api::VoiceAssistantAnnounceFinished msg;
    msg.success = true;
    if (!this->api_client_->send_message(msg)) {
      API_LOG_MSG_DROPPED(TAG, "Announce-finished");
    }
  });
}

void VoiceAssistant::on_event(const api::VoiceAssistantEventResponse &msg) {
  ESP_LOGD(TAG, "Event Type: %" PRId32, msg.event_type);
  switch (msg.event_type) {
    case api::enums::VOICE_ASSISTANT_RUN_START:
      ESP_LOGD(TAG, "Assist Pipeline running");
#ifdef USE_MEDIA_PLAYER
      this->started_streaming_tts_ = false;
      for (const auto &arg : msg.data) {
        if (arg.name == "url") {
          this->tts_response_url_ = arg.value;
        }
      }
#endif
      this->defer([this]() { this->start_trigger_.trigger(); });
      break;
    case api::enums::VOICE_ASSISTANT_WAKE_WORD_START:
      break;
    case api::enums::VOICE_ASSISTANT_WAKE_WORD_END: {
      ESP_LOGD(TAG, "Wake word detected");
      this->defer([this]() { this->wake_word_detected_trigger_.trigger(); });
      break;
    }
    case api::enums::VOICE_ASSISTANT_STT_START:
      ESP_LOGD(TAG, "STT started");
      this->defer([this]() { this->listening_trigger_.trigger(); });
      break;
    case api::enums::VOICE_ASSISTANT_STT_END: {
      std::string text;
      for (const auto &arg : msg.data) {
        if (arg.name == "text") {
          text = arg.value;
        }
      }
      if (text.empty()) {
        ESP_LOGW(TAG, "No text in STT_END event");
        return;
      } else if (text.length() > 500) {
        text.resize(497);
        text += "...";
      }
      ESP_LOGD(TAG, "Speech recognised as: \"%s\"", text.c_str());
      this->defer([this, text]() { this->stt_end_trigger_.trigger(text); });
      break;
    }
    case api::enums::VOICE_ASSISTANT_INTENT_START:
      ESP_LOGD(TAG, "Intent started");
      this->defer([this]() { this->intent_start_trigger_.trigger(); });
      break;
    case api::enums::VOICE_ASSISTANT_INTENT_PROGRESS: {
      ESP_LOGD(TAG, "Intent progress");
      std::string tts_url_for_trigger;
#ifdef USE_MEDIA_PLAYER
      if (this->media_player_ != nullptr) {
        for (const auto &arg : msg.data) {
          if ((arg.name == "tts_start_streaming") && (arg.value == "1") && !this->tts_response_url_.empty()) {
            this->media_player_response_state_ = MediaPlayerResponseState::URL_SENT;

            this->media_player_->make_call().set_media_url(this->tts_response_url_).set_announcement(true).perform();

            this->started_streaming_tts_ = true;
            this->start_playback_timeout_();

            tts_url_for_trigger = this->tts_response_url_;
            this->tts_response_url_.clear();  // Reset streaming URL
            this->set_state_(State::STREAMING_RESPONSE, State::STREAMING_RESPONSE);
          }
        }
      }
#endif
      this->defer([this, tts_url_for_trigger]() { this->intent_progress_trigger_.trigger(tts_url_for_trigger); });
      break;
    }
    case api::enums::VOICE_ASSISTANT_INTENT_END: {
      for (const auto &arg : msg.data) {
        if (arg.name == "conversation_id") {
          this->conversation_id_ = arg.value;
        } else if (arg.name == "continue_conversation") {
          this->continue_conversation_ = (arg.value == "1");
        }
      }
      this->defer([this]() { this->intent_end_trigger_.trigger(); });
      break;
    }
    case api::enums::VOICE_ASSISTANT_TTS_START: {
      std::string text;
      for (const auto &arg : msg.data) {
        if (arg.name == "text") {
          text = arg.value;
        }
      }
      if (text.empty()) {
        ESP_LOGW(TAG, "No text in TTS_START event");
        return;
      }
      if (text.length() > 500) {
        text.resize(497);
        text += "...";
      }
      ESP_LOGD(TAG, "Response: \"%s\"", text.c_str());
      this->defer([this, text]() {
        this->tts_start_trigger_.trigger(text);
#ifdef USE_SPEAKER
        if (this->speaker_ != nullptr) {
          this->speaker_->start();
        }
#endif
      });
      break;
    }
    case api::enums::VOICE_ASSISTANT_TTS_END: {
      std::string url;
      for (const auto &arg : msg.data) {
        if (arg.name == "url") {
          url = arg.value;
        }
      }
      if (url.empty()) {
        ESP_LOGW(TAG, "No url in TTS_END event");
        return;
      }
      ESP_LOGD(TAG, "Response URL: \"%s\"", url.c_str());
      this->defer([this, url]() {
#ifdef USE_MEDIA_PLAYER
        if ((this->media_player_ != nullptr) && (!this->started_streaming_tts_)) {
          this->media_player_response_state_ = MediaPlayerResponseState::URL_SENT;

          this->media_player_->make_call().set_media_url(url).set_announcement(true).perform();

          this->start_playback_timeout_();
        }
        this->started_streaming_tts_ = false;  // Helps indicate reaching the TTS_END stage
#endif
        this->tts_end_trigger_.trigger(url);
      });
      State new_state = this->local_output_ ? State::STREAMING_RESPONSE : State::IDLE;
      if (new_state != this->state_) {
        // Don't needlessly change the state. The intent progress stage may have already changed the state to
        // streaming response.
        this->set_state_(new_state, new_state);
      }
      break;
    }
    case api::enums::VOICE_ASSISTANT_RUN_END: {
      ESP_LOGD(TAG, "Assist Pipeline ended");
      if ((this->state_ == State::START_PIPELINE) || (this->state_ == State::STARTING_PIPELINE) ||
          (this->state_ == State::STREAMING_MICROPHONE)) {
        // Microphone is running, stop it
        this->set_state_(State::STOP_MICROPHONE, State::IDLE);
      } else if (this->state_ == State::AWAITING_RESPONSE) {
        // No TTS start event ("nevermind")
        this->set_state_(State::IDLE, State::IDLE);
      }
      this->defer([this]() { this->end_trigger_.trigger(); });
      break;
    }
    case api::enums::VOICE_ASSISTANT_ERROR: {
      std::string code;
      std::string message;
      for (const auto &arg : msg.data) {
        if (arg.name == "code") {
          code = arg.value;
        } else if (arg.name == "message") {
          message = arg.value;
        }
      }
      if (code == "wake-word-timeout" || code == "wake_word_detection_aborted" || code == "no_wake_word") {
        // Don't change state here since either the "tts-end" or "run-end" events will do it.
        return;
      } else if (code == "wake-provider-missing" || code == "wake-engine-missing") {
        // Wake word is not set up or not ready on Home Assistant so stop and do not retry until user starts again.
        this->defer([this, code, message]() {
          this->request_stop();
          this->error_trigger_.trigger(code, message);
        });
        return;
      }
      ESP_LOGE(TAG, "Error: %s - %s", code.c_str(), message.c_str());
      if (this->state_ != State::IDLE) {
        this->signal_stop_();
        this->set_state_(State::STOP_MICROPHONE, State::IDLE);
      }
      this->defer([this, code, message]() { this->error_trigger_.trigger(code, message); });
      break;
    }
    case api::enums::VOICE_ASSISTANT_TTS_STREAM_START: {
#ifdef USE_SPEAKER
      if (this->speaker_ != nullptr) {
        this->wait_for_stream_end_ = true;
        ESP_LOGD(TAG, "TTS stream start");
        this->defer([this] { this->tts_stream_start_trigger_.trigger(); });
      }
#endif
      break;
    }
    case api::enums::VOICE_ASSISTANT_TTS_STREAM_END: {
#ifdef USE_SPEAKER
      if (this->speaker_ != nullptr) {
        this->stream_ended_ = true;
        ESP_LOGD(TAG, "TTS stream end");
      }
#endif
      break;
    }
    case api::enums::VOICE_ASSISTANT_STT_VAD_START:
      ESP_LOGD(TAG, "Starting STT by VAD");
      this->defer([this]() { this->stt_vad_start_trigger_.trigger(); });
      break;
    case api::enums::VOICE_ASSISTANT_STT_VAD_END:
      ESP_LOGD(TAG, "STT by VAD end");
      this->set_state_(State::STOP_MICROPHONE, State::AWAITING_RESPONSE);
      this->defer([this]() { this->stt_vad_end_trigger_.trigger(); });
      break;
    default:
      ESP_LOGD(TAG, "Unhandled event type: %" PRId32, msg.event_type);
      break;
  }
}

void VoiceAssistant::on_audio(const api::VoiceAssistantAudio &msg) {
#ifdef USE_SPEAKER  // We should never get to this function if there is no speaker anyway
  if ((this->speaker_ != nullptr) && (this->speaker_buffer_ != nullptr)) {
    if (this->speaker_buffer_index_ + msg.data_len <= SPEAKER_BUFFER_SIZE) {
      memcpy(this->speaker_buffer_ + this->speaker_buffer_index_, msg.data, msg.data_len);
      this->speaker_buffer_index_ += msg.data_len;
      this->speaker_buffer_size_ += msg.data_len;
      this->speaker_bytes_received_ += msg.data_len;
      this->write_speaker_();
      ESP_LOGV(TAG, "Received audio: %u bytes from API", msg.data_len);
    } else {
      ESP_LOGE(TAG, "Cannot receive audio, buffer is full");
    }
  }
#endif
}

void VoiceAssistant::on_timer_event(const api::VoiceAssistantTimerEventResponse &msg) {
  // Find existing timer or add a new one
  auto it = this->timers_.begin();
  for (; it != this->timers_.end(); ++it) {
    if (it->id == msg.timer_id)
      break;
  }
  if (it == this->timers_.end()) {
    this->timers_.push_back({});
    it = this->timers_.end() - 1;
  }
  it->id = msg.timer_id;
  it->name = msg.name;
  it->total_seconds = msg.total_seconds;
  it->seconds_left = msg.seconds_left;
  it->is_active = msg.is_active;

  char timer_buf[Timer::TO_STR_BUFFER_SIZE];
  ESP_LOGD(TAG,
           "Timer Event\n"
           "  Type: %" PRId32 "\n"
           "  %s",
           msg.event_type, it->to_str(timer_buf));

  switch (msg.event_type) {
    case api::enums::VOICE_ASSISTANT_TIMER_STARTED:
      this->timer_started_trigger_.trigger(*it);
      break;
    case api::enums::VOICE_ASSISTANT_TIMER_UPDATED:
      this->timer_updated_trigger_.trigger(*it);
      break;
    case api::enums::VOICE_ASSISTANT_TIMER_CANCELLED:
      this->timer_cancelled_trigger_.trigger(*it);
      this->timers_.erase(it);
      break;
    case api::enums::VOICE_ASSISTANT_TIMER_FINISHED:
      this->timer_finished_trigger_.trigger(*it);
      this->timers_.erase(it);
      break;
  }

  if (this->timers_.empty()) {
    this->cancel_interval("timer-event");
    this->timer_tick_running_ = false;
  } else if (!this->timer_tick_running_) {
    this->set_interval("timer-event", 1000, [this]() { this->timer_tick_(); });
    this->timer_tick_running_ = true;
  }
}

void VoiceAssistant::timer_tick_() {
  for (auto &timer : this->timers_) {
    if (timer.is_active && timer.seconds_left > 0) {
      timer.seconds_left--;
    }
  }
  this->timer_tick_trigger_.trigger(this->timers_);
}

void VoiceAssistant::on_announce(const api::VoiceAssistantAnnounceRequest &msg) {
#ifdef USE_MEDIA_PLAYER
  if (this->media_player_ != nullptr) {
    this->tts_start_trigger_.trigger(msg.text);

    this->media_player_response_state_ = MediaPlayerResponseState::URL_SENT;

    if (!msg.preannounce_media_id.empty()) {
      this->media_player_->make_call().set_media_url(msg.preannounce_media_id).set_announcement(true).perform();
    }
    // Enqueueing a URL with an empty playlist will still play the file immediately
    this->media_player_->make_call()
        .set_command(media_player::MEDIA_PLAYER_COMMAND_ENQUEUE)
        .set_media_url(msg.media_id)
        .set_announcement(true)
        .perform();
    this->continue_conversation_ = msg.start_conversation;

    this->start_playback_timeout_();

    if (this->continuous_) {
      this->set_state_(State::STOP_MICROPHONE, State::STREAMING_RESPONSE);
    } else {
      this->set_state_(State::STREAMING_RESPONSE, State::STREAMING_RESPONSE);
    }

    this->tts_end_trigger_.trigger(msg.media_id);
    this->end_trigger_.trigger();
  }
#endif
}

void VoiceAssistant::on_set_configuration(const std::vector<std::string> &active_wake_words) {
#ifdef USE_MICRO_WAKE_WORD
  if (this->micro_wake_word_) {
    // Disable all wake words first
    for (auto &model : this->micro_wake_word_->get_wake_words()) {
      model->disable();
    }

    // Enable only active wake words
    for (const auto &ww_id : active_wake_words) {
      for (auto &model : this->micro_wake_word_->get_wake_words()) {
        if (model->get_id() == ww_id) {
          model->enable();
          ESP_LOGD(TAG, "Enabled wake word: %s (id=%s)", model->get_wake_word().c_str(), model->get_id().c_str());
        }
      }
    }
  }
#endif
};

const Configuration &VoiceAssistant::get_configuration() {
  this->config_.available_wake_words.clear();
  this->config_.active_wake_words.clear();

#ifdef USE_MICRO_WAKE_WORD
  if (this->micro_wake_word_) {
    this->config_.max_active_wake_words = 1;

    for (auto &model : this->micro_wake_word_->get_wake_words()) {
      if (model->is_enabled()) {
        this->config_.active_wake_words.push_back(model->get_id());
      }

      WakeWord wake_word;
      wake_word.id = model->get_id();
      wake_word.wake_word = model->get_wake_word();
      for (const auto &lang : model->get_trained_languages()) {
        wake_word.trained_languages.push_back(lang);
      }
      this->config_.available_wake_words.push_back(std::move(wake_word));
    }
  } else {
#endif
    // No microWakeWord
    this->config_.max_active_wake_words = 0;
#ifdef USE_MICRO_WAKE_WORD
  }
#endif

  return this->config_;
};

VoiceAssistant *global_voice_assistant = nullptr;  // NOLINT(cppcoreguidelines-avoid-non-const-global-variables)

}  // namespace esphome::voice_assistant

#endif  // USE_VOICE_ASSISTANT
