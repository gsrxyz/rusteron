// code here is included in all modules and extends generated classes
pub static AERON_IPC_STREAM: &std::ffi::CStr =
    unsafe { std::ffi::CStr::from_bytes_with_nul_unchecked(b"aeron:ipc\0") };

unsafe impl Send for AeronCountersReader {}
unsafe impl Send for AeronSubscription {}
unsafe impl Sync for AeronSubscription {}
unsafe impl Send for AeronPublication {}
unsafe impl Sync for AeronPublication {}
unsafe impl Send for AeronExclusivePublication {}
unsafe impl Sync for AeronExclusivePublication {}
unsafe impl Send for AeronCounter {}
unsafe impl Sync for AeronCounter {}

impl AeronCnc {
    /// Note this allocates the rust component on stack but the C aeron_cnc_t struct is still on the heap,
    /// as Aeron does the allocation.
    #[inline]
    pub fn read_on_partial_stack(
        aeron_dir: &std::ffi::CString,
        mut handler: impl FnMut(&mut AeronCnc),
    ) -> Result<(), AeronCError> {
        let cnc = ManagedCResource::initialise(move |cnc| unsafe {
          aeron_cnc_init(cnc, aeron_dir.as_ptr(), 0)
        })?;
        let mut cnc = Self {
          inner: CResource::Borrowed(cnc),
        };
        handler(&mut cnc);
        unsafe { aeron_cnc_close(cnc.get_inner()) };
        Ok(())
    }

    /// **Deprecated**: allocate on the heap. Use `new_on_heap` instead.
    #[deprecated(since = "0.1.122", note = "Use `new_on_heap` instead")]
    #[inline]
    pub fn new(aeron_dir: &str) -> Result<AeronCnc, AeronCError> {
        Self::new_on_heap(aeron_dir)
    }

    /// Note this allocates on the heap, cannot be stored this on stack. As Aeron will do the allocation.
    /// Try to use `read_on_partial_stack` which performs less allocations
    #[inline]
    pub fn new_on_heap(aeron_dir: &str) -> Result<AeronCnc, AeronCError> {
        let c_string = std::ffi::CString::new(aeron_dir).expect("CString conversion failed");
        let resource = ManagedCResource::new(
          move |cnc| unsafe { aeron_cnc_init(cnc, c_string.as_ptr(), 0) },
          Some(Box::new(move |cnc| unsafe {
              aeron_cnc_close(*cnc);
              0
          })),
          false,
          None,
        )?;

        let result = Self {
          inner: CResource::OwnedOnHeap(std::rc::Rc::new(resource)),
        };
        Ok(result)
    }

    #[doc = " Gets the timestamp of the last heartbeat sent to the media driver from any client.\n\n @param aeron_cnc to query\n @return last heartbeat timestamp in ms."]
    #[inline]
    pub fn get_to_driver_heartbeat_ms(&self) -> Result<i64, AeronCError> {
        unsafe {
          let timestamp = aeron_cnc_to_driver_heartbeat(self.get_inner());
          if timestamp >= 0 {
              return Ok(timestamp);
          } else {
              return Err(AeronCError::from_code(timestamp as i32));
          }
        }
    }
}

#[repr(u32)]
#[derive(Debug, Copy, Clone, Hash, PartialEq, Eq)]
pub enum AeronSystemCounterType {
    /// Running total of bytes sent for data over UDP, excluding IP headers.
    BytesSent = 0,
    /// Running total of bytes received for data over UDP, excluding IP headers.
    BytesReceived = 1,
    /// Failed offers to the receiver proxy suggesting back-pressure.
    ReceiverProxyFails = 2,
    /// Failed offers to the sender proxy suggesting back-pressure.
    SenderProxyFails = 3,
    /// Failed offers to the driver conductor proxy suggesting back-pressure.
    ConductorProxyFails = 4,
    /// Count of NAKs sent back to senders requesting re-transmits.
    NakMessagesSent = 5,
    /// Count of NAKs received from receivers requesting re-transmits.
    NakMessagesReceived = 6,
    /// Count of status messages sent back to senders for flow control.
    StatusMessagesSent = 7,
    /// Count of status messages received from receivers for flow control.
    StatusMessagesReceived = 8,
    /// Count of heartbeat data frames sent to indicate liveness in the absence of data to send.
    HeartbeatsSent = 9,
    /// Count of heartbeat data frames received to indicate liveness in the absence of data to send.
    HeartbeatsReceived = 10,
    /// Count of data packets re-transmitted as a result of NAKs.
    RetransmitsSent = 11,
    /// Count of packets received which under-run the current flow control window for images.
    FlowControlUnderRuns = 12,
    /// Count of packets received which over-run the current flow control window for images.
    FlowControlOverRuns = 13,
    /// Count of invalid packets received.
    InvalidPackets = 14,
    /// Count of errors observed by the driver and an indication to read the distinct error log.
    Errors = 15,
    /// Count of socket send operations which resulted in less than the packet length being sent.
    ShortSends = 16,
    /// Count of attempts to free log buffers no longer required by the driver that are still held by clients.
    FreeFails = 17,
    /// Count of the times a sender has entered the state of being back-pressured when it could have sent faster.
    SenderFlowControlLimits = 18,
    /// Count of the times a publication has been unblocked after a client failed to complete an offer within a timeout.
    UnblockedPublications = 19,
    /// Count of the times a command has been unblocked after a client failed to complete an offer within a timeout.
    UnblockedCommands = 20,
    /// Count of the times the channel endpoint detected a possible TTL asymmetry between its config and a new connection.
    PossibleTtlAsymmetry = 21,
    /// Current status of the ControllableIdleStrategy if configured.
    ControllableIdleStrategy = 22,
    /// Count of the times a loss gap has been filled when NAKs have been disabled.
    LossGapFills = 23,
    /// Count of the Aeron clients that have timed out without a graceful close.
    ClientTimeouts = 24,
    /// Count of the times a connection endpoint has been re-resolved resulting in a change.
    ResolutionChanges = 25,
    /// The maximum time spent by the conductor between work cycles.
    ConductorMaxCycleTime = 26,
    /// Count of the number of times the cycle time threshold has been exceeded by the conductor in its work cycle.
    ConductorCycleTimeThresholdExceeded = 27,
    /// The maximum time spent by the sender between work cycles.
    SenderMaxCycleTime = 28,
    /// Count of the number of times the cycle time threshold has been exceeded by the sender in its work cycle.
    SenderCycleTimeThresholdExceeded = 29,
    /// The maximum time spent by the receiver between work cycles.
    ReceiverMaxCycleTime = 30,
    /// Count of the number of times the cycle time threshold has been exceeded by the receiver in its work cycle.
    ReceiverCycleTimeThresholdExceeded = 31,
    /// The maximum time spent by the NameResolver in one of its operations.
    NameResolverMaxTime = 32,
    /// Count of the number of times the time threshold has been exceeded by the NameResolver.
    NameResolverTimeThresholdExceeded = 33,
    /// The version of the media driver.
    AeronVersion = 34,
    /// The total number of bytes currently mapped in log buffers, the CnC file, and the loss report.
    BytesCurrentlyMapped = 35,
    /// A minimum bound on the number of bytes re-transmitted as a result of NAKs.\n///\n/// MDC retransmits are only counted once; therefore, this is a minimum bound rather than the actual number\n/// of retransmitted bytes. Note that retransmitted bytes are not included in the `BytesSent` counter value.
    RetransmittedBytes = 36,
    /// A count of the number of times that the retransmit pool has been overflowed.
    RetransmitOverflow = 37,
    /// A count of the number of error frames received by this driver.
    ErrorFramesReceived = 38,
    /// A count of the number of error frames sent by this driver.
    ErrorFramesSent = 39,
    DummyLast = 40,
}

impl std::convert::TryFrom<i32> for AeronSystemCounterType {
    type Error = AeronCError;

    fn try_from(value: i32) -> Result<Self, Self::Error> {
        if value < 0 {
          return Err(AeronCError::from_code(value));
        }
        match value as u32 {
          0 => Ok(AeronSystemCounterType::BytesSent),
          1 => Ok(AeronSystemCounterType::BytesReceived),
          2 => Ok(AeronSystemCounterType::ReceiverProxyFails),
          3 => Ok(AeronSystemCounterType::SenderProxyFails),
          4 => Ok(AeronSystemCounterType::ConductorProxyFails),
          5 => Ok(AeronSystemCounterType::NakMessagesSent),
          6 => Ok(AeronSystemCounterType::NakMessagesReceived),
          7 => Ok(AeronSystemCounterType::StatusMessagesSent),
          8 => Ok(AeronSystemCounterType::StatusMessagesReceived),
          9 => Ok(AeronSystemCounterType::HeartbeatsSent),
          10 => Ok(AeronSystemCounterType::HeartbeatsReceived),
          11 => Ok(AeronSystemCounterType::RetransmitsSent),
          12 => Ok(AeronSystemCounterType::FlowControlUnderRuns),
          13 => Ok(AeronSystemCounterType::FlowControlOverRuns),
          14 => Ok(AeronSystemCounterType::InvalidPackets),
          15 => Ok(AeronSystemCounterType::Errors),
          16 => Ok(AeronSystemCounterType::ShortSends),
          17 => Ok(AeronSystemCounterType::FreeFails),
          18 => Ok(AeronSystemCounterType::SenderFlowControlLimits),
          19 => Ok(AeronSystemCounterType::UnblockedPublications),
          20 => Ok(AeronSystemCounterType::UnblockedCommands),
          21 => Ok(AeronSystemCounterType::PossibleTtlAsymmetry),
          22 => Ok(AeronSystemCounterType::ControllableIdleStrategy),
          23 => Ok(AeronSystemCounterType::LossGapFills),
          24 => Ok(AeronSystemCounterType::ClientTimeouts),
          25 => Ok(AeronSystemCounterType::ResolutionChanges),
          26 => Ok(AeronSystemCounterType::ConductorMaxCycleTime),
          27 => Ok(AeronSystemCounterType::ConductorCycleTimeThresholdExceeded),
          28 => Ok(AeronSystemCounterType::SenderMaxCycleTime),
          29 => Ok(AeronSystemCounterType::SenderCycleTimeThresholdExceeded),
          30 => Ok(AeronSystemCounterType::ReceiverMaxCycleTime),
          31 => Ok(AeronSystemCounterType::ReceiverCycleTimeThresholdExceeded),
          32 => Ok(AeronSystemCounterType::NameResolverMaxTime),
          33 => Ok(AeronSystemCounterType::NameResolverTimeThresholdExceeded),
          34 => Ok(AeronSystemCounterType::AeronVersion),
          35 => Ok(AeronSystemCounterType::BytesCurrentlyMapped),
          36 => Ok(AeronSystemCounterType::RetransmittedBytes),
          37 => Ok(AeronSystemCounterType::RetransmitOverflow),
          38 => Ok(AeronSystemCounterType::ErrorFramesReceived),
          39 => Ok(AeronSystemCounterType::ErrorFramesSent),
          40 => Ok(AeronSystemCounterType::DummyLast),
          _ => Err(AeronCError::from_code(-1)),
        }
    }
}

impl AeronCncMetadata {
    #[inline]
    /// allocates on heap
    pub fn load_from_file(aeron_dir: &str) -> Result<Self, AeronCError> {
        let aeron_dir = std::ffi::CString::new(aeron_dir).expect("CString::new failed");
        let mapped_file = std::rc::Rc::new(std::cell::RefCell::new(aeron_mapped_file_t {
          addr: std::ptr::null_mut(),
          length: 0,
        }));
        let mapped_file2 = std::rc::Rc::clone(&mapped_file);
        let resource = ManagedCResource::new(
          move |ctx| {
              let result = unsafe {
                aeron_cnc_map_file_and_load_metadata(
                    aeron_dir.as_ptr(),
                    mapped_file.borrow_mut().deref_mut() as *mut aeron_mapped_file_t,
                    ctx,
                )
              };
              if result == aeron_cnc_load_result_t::AERON_CNC_LOAD_SUCCESS {
                1
              } else {
                -1
              }
          },
          Some(Box::new(move |ctx| unsafe {
              aeron_unmap(mapped_file2.borrow_mut().deref_mut() as *mut aeron_mapped_file_t)
          })),
          false,
          None,
        )?;

        let result = Self {
          inner: CResource::OwnedOnHeap(std::rc::Rc::new(resource)),
        };
        Ok(result)
    }

    #[inline]
    /// allocates on stack
    pub fn read_from_file(
        aeron_dir: &std::ffi::CString,
        mut handler: impl FnMut(Self),
    ) -> Result<(), AeronCError> {
        let mut mapped_file = aeron_mapped_file_t {
          addr: std::ptr::null_mut(),
          length: 0,
        };
        let ctx = ManagedCResource::initialise(move |ctx| {
          let result = unsafe {
              aeron_cnc_map_file_and_load_metadata(
                aeron_dir.as_ptr(),
                &mut mapped_file as *mut aeron_mapped_file_t,
                ctx,
              )
          };
          if result == aeron_cnc_load_result_t::AERON_CNC_LOAD_SUCCESS {
              1
          } else {
              -1
          }
        })?;

        let result = Self {
          inner: CResource::Borrowed(ctx),
        };

        handler(result);
        unsafe { aeron_unmap(&mut mapped_file as *mut aeron_mapped_file_t) };
        Ok(())
    }
}

impl AeronSubscription {
    pub fn close_with_no_args(&mut self) -> Result<(), AeronCError> {
        self.close(Handlers::no_notification_handler())?;
        Ok(())
    }
}

impl AeronPublication {
    pub fn close_with_no_args(&self) -> Result<(), AeronCError> {
        self.close(Handlers::no_notification_handler())?;
        Ok(())
    }

    /// sometimes when you first connect, is_connected = true, but you get backpressure as position is 0
    /// this will check if both publication is connected and position > 0
    #[inline]
    pub fn is_ready(&self) -> bool {
        self.is_connected() && self.position_limit() != 0
    }
}

impl AeronExclusivePublication {
    pub fn close_with_no_args(&self) -> Result<(), AeronCError> {
        self.close(Handlers::no_notification_handler())?;
        Ok(())
    }

    /// sometimes when you first connect, is_connected = true, but you get backpressure as position is 0
    /// this will check if both publication is connected and position > 0
    #[inline]
    pub fn is_ready(&self) -> bool {
        self.is_connected() && self.position_limit() != 0
    }
}

impl AeronCounter {
    pub fn close_with_no_args(&self) -> Result<(), AeronCError> {
        self.close(Handlers::no_notification_handler())?;
        Ok(())
    }
}

impl AeronCounter {
    #[inline]
    pub fn addr_atomic(&self) -> &std::sync::atomic::AtomicI64 {
        unsafe { std::sync::atomic::AtomicI64::from_ptr(self.addr()) }
    }
}

impl AeronSubscription {
    pub fn async_add_destination(
        &mut self,
        client: &Aeron,
        destination: &std::ffi::CStr,
    ) -> Result<AeronAsyncDestination, AeronCError> {
        AeronAsyncDestination::aeron_subscription_async_add_destination(client, self, destination)
    }

    pub fn add_destination(
        &mut self,
        client: &Aeron,
        destination: &std::ffi::CStr,
        timeout: std::time::Duration,
    ) -> Result<(), AeronCError> {
        let result = self.async_add_destination(client, destination)?;
        if result
          .aeron_subscription_async_destination_poll()
          .unwrap_or_default()
          > 0
        {
          return Ok(());
        }
        let time = std::time::Instant::now();
        while time.elapsed() < timeout {
          if result
              .aeron_subscription_async_destination_poll()
              .unwrap_or_default()
              > 0
          {
              return Ok(());
          }
          #[cfg(debug_assertions)]
          std::thread::sleep(std::time::Duration::from_millis(10));
        }
        log::error!("failed async poll for {:?} {:?}", destination, self);
        Err(AeronErrorType::TimedOut.into())
    }
}

impl AeronExclusivePublication {
    pub fn async_add_destination(
        &mut self,
        client: &Aeron,
        destination: &std::ffi::CStr,
    ) -> Result<AeronAsyncDestination, AeronCError> {
        AeronAsyncDestination::aeron_exclusive_publication_async_add_destination(
          client,
          self,
          destination,
        )
    }

    pub fn add_destination(
        &mut self,
        client: &Aeron,
        destination: &std::ffi::CStr,
        timeout: std::time::Duration,
    ) -> Result<(), AeronCError> {
        let result = self.async_add_destination(client, destination)?;
        if result
          .aeron_subscription_async_destination_poll()
          .unwrap_or_default()
          > 0
        {
          return Ok(());
        }
        let time = std::time::Instant::now();
        while time.elapsed() < timeout {
          if result
              .aeron_subscription_async_destination_poll()
              .unwrap_or_default()
              > 0
          {
              return Ok(());
          }
          #[cfg(debug_assertions)]
          std::thread::sleep(std::time::Duration::from_millis(10));
        }
        log::error!("failed async poll for {:?} {:?}", destination, self);
        Err(AeronErrorType::TimedOut.into())
    }
}

impl AeronPublication {
    pub fn async_add_destination(
        &mut self,
        client: &Aeron,
        destination: &std::ffi::CStr,
    ) -> Result<AeronAsyncDestination, AeronCError> {
        AeronAsyncDestination::aeron_publication_async_add_destination(client, self, destination)
    }

    pub fn add_destination(
        &mut self,
        client: &Aeron,
        destination: &std::ffi::CStr,
        timeout: std::time::Duration,
    ) -> Result<(), AeronCError> {
        let result = self.async_add_destination(client, destination)?;
        if result
          .aeron_subscription_async_destination_poll()
          .unwrap_or_default()
          > 0
        {
          return Ok(());
        }
        let time = std::time::Instant::now();
        while time.elapsed() < timeout {
          if result
              .aeron_subscription_async_destination_poll()
              .unwrap_or_default()
              > 0
          {
              return Ok(());
          }
          #[cfg(debug_assertions)]
          std::thread::sleep(std::time::Duration::from_millis(10));
        }
        log::error!("failed async poll for {:?} {:?}", destination, self);
        Err(AeronErrorType::TimedOut.into())
    }
}

impl std::str::FromStr for AeronUriStringBuilder {
    type Err = AeronCError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let builder = AeronUriStringBuilder::default();
        let s = std::ffi::CString::new(s).expect("CString::new failed");
        builder.init_on_string(&s)?;
        Ok(builder)
    }
}

// AeronUriStringBuilder does not follow convention so manually adding Default method which calls close
impl Default for AeronUriStringBuilder {
    fn default() -> Self {
        let r_constructor = ManagedCResource::new(
          move |ctx_field| {
              let inst: aeron_uri_string_builder_t = unsafe { std::mem::zeroed() };
              let inner_ptr: *mut aeron_uri_string_builder_t = Box::into_raw(Box::new(inst));
              unsafe { *ctx_field = inner_ptr };
              0
          },
          Some(Box::new(move |ctx_field| unsafe {
              aeron_uri_string_builder_close(*ctx_field)
          })),
          true,
          Some(|ctx| unsafe { (*ctx).closed })
        ).expect("should not happen");
        Self {
          inner: CResource::OwnedOnHeap(std::rc::Rc::new(r_constructor)),
        }
    }
}

impl AeronCError {
    pub fn get_last_err_message(&self) -> &str {
        Aeron::errmsg()
    }
}

const PARSE_CSTR_ERROR_CODE: i32 = -132131;

impl AeronUriStringBuilder {
    #[inline]
    pub fn build(&self, max_str_length: usize) -> Result<String, AeronCError> {
        let mut result = String::with_capacity(max_str_length);
        self.build_into(&mut result)?;
        Ok(result)
    }

    pub fn put_string(&self, key: &std::ffi::CStr, value: &str) -> Result<&Self, AeronCError> {
        let value = std::ffi::CString::new(value)
          .map_err(|_| AeronCError::from_code(PARSE_CSTR_ERROR_CODE))?;
        self.put(&key, &value)?;
        Ok(self)
    }

    pub fn put_strings(&self, key: &str, value: &str) -> Result<&Self, AeronCError> {
        let key = std::ffi::CString::new(key)
          .map_err(|_| AeronCError::from_code(PARSE_CSTR_ERROR_CODE))?;
        let value = std::ffi::CString::new(value)
          .map_err(|_| AeronCError::from_code(PARSE_CSTR_ERROR_CODE))?;
        self.put(&key, &value)?;
        Ok(self)
    }

    pub fn media(&self, value: Media) -> Result<&Self, AeronCError> {
        let key = std::ffi::CStr::from_bytes_until_nul(AERON_URI_STRING_BUILDER_MEDIA_KEY)
          .map_err(|_| AeronCError::from_code(PARSE_CSTR_ERROR_CODE))?;
        self.put_string(key, value.as_str())?;
        Ok(self)
    }

    pub fn control_mode(&self, value: ControlMode) -> Result<&Self, AeronCError> {
        let key = std::ffi::CStr::from_bytes_until_nul(AERON_UDP_CHANNEL_CONTROL_MODE_KEY)
          .map_err(|_| AeronCError::from_code(PARSE_CSTR_ERROR_CODE))?;
        self.put_string(key, value.as_str())?;
        Ok(self)
    }

    pub fn prefix(&self, value: &str) -> Result<&Self, AeronCError> {
        let key = std::ffi::CStr::from_bytes_until_nul(AERON_URI_STRING_BUILDER_PREFIX_KEY)
          .map_err(|_| AeronCError::from_code(PARSE_CSTR_ERROR_CODE))?;
        self.put_string(key, value)?;
        Ok(self)
    }

    pub fn initial_term_id(&self, value: i32) -> Result<&Self, AeronCError> {
        let key = std::ffi::CStr::from_bytes_until_nul(AERON_URI_INITIAL_TERM_ID_KEY)
          .map_err(|_| AeronCError::from_code(PARSE_CSTR_ERROR_CODE))?;
        self.put_int32(key, value)?;
        Ok(self)
    }
    pub fn term_id(&self, value: i32) -> Result<&Self, AeronCError> {
        let key = std::ffi::CStr::from_bytes_until_nul(AERON_URI_TERM_ID_KEY)
          .map_err(|_| AeronCError::from_code(PARSE_CSTR_ERROR_CODE))?;
        self.put_int32(key, value)?;
        Ok(self)
    }
    pub fn term_offset(&self, value: i32) -> Result<&Self, AeronCError> {
        let key = std::ffi::CStr::from_bytes_until_nul(AERON_URI_TERM_OFFSET_KEY)
          .map_err(|_| AeronCError::from_code(PARSE_CSTR_ERROR_CODE))?;
        self.put_int32(key, value)?;
        Ok(self)
    }
    pub fn alias(&self, value: &str) -> Result<&Self, AeronCError> {
        let key = std::ffi::CStr::from_bytes_until_nul(AERON_URI_ALIAS_KEY)
          .map_err(|_| AeronCError::from_code(PARSE_CSTR_ERROR_CODE))?;
        self.put_string(key, value)?;
        Ok(self)
    }
    pub fn term_length(&self, value: &str) -> Result<&Self, AeronCError> {
        let key = std::ffi::CStr::from_bytes_until_nul(AERON_URI_TERM_LENGTH_KEY)
          .map_err(|_| AeronCError::from_code(PARSE_CSTR_ERROR_CODE))?;
        self.put_string(key, value)?;
        Ok(self)
    }
    pub fn linger_timeout(&self, value: i64) -> Result<&Self, AeronCError> {
        let key = std::ffi::CStr::from_bytes_until_nul(AERON_URI_LINGER_TIMEOUT_KEY)
          .map_err(|_| AeronCError::from_code(PARSE_CSTR_ERROR_CODE))?;
        self.put_int64(key, value)?;
        Ok(self)
    }
    pub fn mtu_length(&self, value: i32) -> Result<&Self, AeronCError> {
        let key = std::ffi::CStr::from_bytes_until_nul(AERON_URI_MTU_LENGTH_KEY)
          .map_err(|_| AeronCError::from_code(PARSE_CSTR_ERROR_CODE))?;
        self.put_int32(key, value)?;
        Ok(self)
    }
    pub fn ttl(&self, value: i32) -> Result<&Self, AeronCError> {
        let key = std::ffi::CStr::from_bytes_until_nul(AERON_UDP_CHANNEL_TTL_KEY)
          .map_err(|_| AeronCError::from_code(PARSE_CSTR_ERROR_CODE))?;
        self.put_int32(key, value)?;
        Ok(self)
    }
    pub fn sparse_term(&self, value: bool) -> Result<&Self, AeronCError> {
        let key = std::ffi::CStr::from_bytes_until_nul(AERON_URI_SPARSE_TERM_KEY)
          .map_err(|_| AeronCError::from_code(PARSE_CSTR_ERROR_CODE))?;
        self.put_string(key, if value { "true" } else { "false" })?;
        Ok(self)
    }
    pub fn reliable(&self, value: bool) -> Result<&Self, AeronCError> {
        let key = std::ffi::CStr::from_bytes_until_nul(AERON_UDP_CHANNEL_RELIABLE_KEY)
          .map_err(|_| AeronCError::from_code(PARSE_CSTR_ERROR_CODE))?;
        self.put_string(key, if value { "true" } else { "false" })?;
        Ok(self)
    }
    pub fn eos(&self, value: bool) -> Result<&Self, AeronCError> {
        let key = std::ffi::CStr::from_bytes_until_nul(AERON_URI_EOS_KEY)
          .map_err(|_| AeronCError::from_code(PARSE_CSTR_ERROR_CODE))?;
        self.put_string(key, if value { "true" } else { "false" })?;
        Ok(self)
    }
    pub fn tether(&self, value: bool) -> Result<&Self, AeronCError> {
        let key = std::ffi::CStr::from_bytes_until_nul(AERON_URI_TETHER_KEY)
          .map_err(|_| AeronCError::from_code(PARSE_CSTR_ERROR_CODE))?;
        self.put_string(key, if value { "true" } else { "false" })?;
        Ok(self)
    }
    pub fn tags(&self, value: &str) -> Result<&Self, AeronCError> {
        let key = std::ffi::CStr::from_bytes_until_nul(AERON_URI_TAGS_KEY)
          .map_err(|_| AeronCError::from_code(PARSE_CSTR_ERROR_CODE))?;
        self.put_string(key, value)?;
        Ok(self)
    }
    pub fn endpoint(&self, value: &str) -> Result<&Self, AeronCError> {
        let key = std::ffi::CStr::from_bytes_until_nul(AERON_UDP_CHANNEL_ENDPOINT_KEY)
          .map_err(|_| AeronCError::from_code(PARSE_CSTR_ERROR_CODE))?;
        self.put_string(key, value)?;
        Ok(self)
    }
    pub fn interface(&self, value: &str) -> Result<&Self, AeronCError> {
        let key = std::ffi::CStr::from_bytes_until_nul(AERON_UDP_CHANNEL_INTERFACE_KEY)
          .map_err(|_| AeronCError::from_code(PARSE_CSTR_ERROR_CODE))?;
        self.put_string(key, value)?;
        Ok(self)
    }
    pub fn control(&self, value: &str) -> Result<&Self, AeronCError> {
        let key = std::ffi::CStr::from_bytes_until_nul(AERON_UDP_CHANNEL_CONTROL_KEY)
          .map_err(|_| AeronCError::from_code(PARSE_CSTR_ERROR_CODE))?;
        self.put_string(key, value)?;
        Ok(self)
    }
    pub fn session_id(&self, value: &str) -> Result<&Self, AeronCError> {
        let key = std::ffi::CStr::from_bytes_until_nul(AERON_URI_SESSION_ID_KEY)
          .map_err(|_| AeronCError::from_code(PARSE_CSTR_ERROR_CODE))?;
        self.put_string(key, value)?;
        Ok(self)
    }
    pub fn group(&self, value: bool) -> Result<&Self, AeronCError> {
        let key = std::ffi::CStr::from_bytes_until_nul(AERON_URI_GROUP_KEY)
          .map_err(|_| AeronCError::from_code(PARSE_CSTR_ERROR_CODE))?;
        self.put_string(key, if value { "true" } else { "false" })?;
        Ok(self)
    }
    pub fn rejoin(&self, value: bool) -> Result<&Self, AeronCError> {
        let key = std::ffi::CStr::from_bytes_until_nul(AERON_URI_REJOIN_KEY)
          .map_err(|_| AeronCError::from_code(PARSE_CSTR_ERROR_CODE))?;
        self.put_string(key, if value { "true" } else { "false" })?;
        Ok(self)
    }
    pub fn fc(&self, value: &str) -> Result<&Self, AeronCError> {
        let key = std::ffi::CStr::from_bytes_until_nul(AERON_URI_FC_KEY)
          .map_err(|_| AeronCError::from_code(PARSE_CSTR_ERROR_CODE))?;
        self.put_string(key, value)?;
        Ok(self)
    }
    pub fn gtag(&self, value: &str) -> Result<&Self, AeronCError> {
        let key = std::ffi::CStr::from_bytes_until_nul(AERON_URI_GTAG_KEY)
          .map_err(|_| AeronCError::from_code(PARSE_CSTR_ERROR_CODE))?;
        self.put_string(key, value)?;
        Ok(self)
    }
    pub fn cc(&self, value: &str) -> Result<&Self, AeronCError> {
        let key = std::ffi::CStr::from_bytes_until_nul(AERON_URI_CC_KEY)
          .map_err(|_| AeronCError::from_code(PARSE_CSTR_ERROR_CODE))?;
        self.put_string(key, value)?;
        Ok(self)
    }
    pub fn spies_simulate_connection(&self, value: bool) -> Result<&Self, AeronCError> {
        let key = std::ffi::CStr::from_bytes_until_nul(AERON_URI_SPIES_SIMULATE_CONNECTION_KEY)
          .map_err(|_| AeronCError::from_code(PARSE_CSTR_ERROR_CODE))?;
        self.put_string(key, if value { "true" } else { "false" })?;
        Ok(self)
    }
    pub fn ats(&self, value: bool) -> Result<&Self, AeronCError> {
        let key = std::ffi::CStr::from_bytes_until_nul(AERON_URI_ATS_KEY)
          .map_err(|_| AeronCError::from_code(PARSE_CSTR_ERROR_CODE))?;
        self.put_string(key, if value { "true" } else { "false" })?;
        Ok(self)
    }
    pub fn socket_sndbuf(&self, value: i32) -> Result<&Self, AeronCError> {
        let key = std::ffi::CStr::from_bytes_until_nul(AERON_URI_SOCKET_SNDBUF_KEY)
          .map_err(|_| AeronCError::from_code(PARSE_CSTR_ERROR_CODE))?;
        self.put_int32(key, value)?;
        Ok(self)
    }
    pub fn socket_rcvbuf(&self, value: i32) -> Result<&Self, AeronCError> {
        let key = std::ffi::CStr::from_bytes_until_nul(AERON_URI_SOCKET_RCVBUF_KEY)
          .map_err(|_| AeronCError::from_code(PARSE_CSTR_ERROR_CODE))?;
        self.put_int32(key, value)?;
        Ok(self)
    }
    pub fn receiver_window(&self, value: i32) -> Result<&Self, AeronCError> {
        let key = std::ffi::CStr::from_bytes_until_nul(AERON_URI_RECEIVER_WINDOW_KEY)
          .map_err(|_| AeronCError::from_code(PARSE_CSTR_ERROR_CODE))?;
        self.put_int32(key, value)?;
        Ok(self)
    }
    pub fn media_rcv_timestamp_offset(&self, value: &str) -> Result<&Self, AeronCError> {
        let key = std::ffi::CStr::from_bytes_until_nul(AERON_URI_MEDIA_RCV_TIMESTAMP_OFFSET_KEY)
          .map_err(|_| AeronCError::from_code(PARSE_CSTR_ERROR_CODE))?;
        self.put_string(key, value)?;
        Ok(self)
    }
    pub fn channel_rcv_timestamp_offset(&self, value: &str) -> Result<&Self, AeronCError> {
        let key = std::ffi::CStr::from_bytes_until_nul(AERON_URI_CHANNEL_RCV_TIMESTAMP_OFFSET_KEY)
          .map_err(|_| AeronCError::from_code(PARSE_CSTR_ERROR_CODE))?;
        self.put_string(key, value)?;
        Ok(self)
    }
    pub fn channel_snd_timestamp_offset(&self, value: &str) -> Result<&Self, AeronCError> {
        let key = std::ffi::CStr::from_bytes_until_nul(AERON_URI_CHANNEL_SND_TIMESTAMP_OFFSET_KEY)
          .map_err(|_| AeronCError::from_code(PARSE_CSTR_ERROR_CODE))?;
        self.put_string(key, value)?;
        Ok(self)
    }
    pub fn timestamp_offset_reserved(&self, value: &str) -> Result<&Self, AeronCError> {
        let key = std::ffi::CStr::from_bytes_until_nul(AERON_URI_TIMESTAMP_OFFSET_RESERVED)
          .map_err(|_| AeronCError::from_code(PARSE_CSTR_ERROR_CODE))?;
        self.put_string(key, value)?;
        Ok(self)
    }
    pub fn response_correlation_id(&self, value: i64) -> Result<&Self, AeronCError> {
        let key = std::ffi::CStr::from_bytes_until_nul(AERON_URI_RESPONSE_CORRELATION_ID_KEY)
          .map_err(|_| AeronCError::from_code(PARSE_CSTR_ERROR_CODE))?;
        self.put_int64(key, value)?;
        Ok(self)
    }
    pub fn nak_delay(&self, value: i64) -> Result<&Self, AeronCError> {
        let key = std::ffi::CStr::from_bytes_until_nul(AERON_URI_NAK_DELAY_KEY)
            .map_err(|_| AeronCError::from_code(PARSE_CSTR_ERROR_CODE))?;
        self.put_int64(key, value)?;
        Ok(self)
    }
    pub fn untethered_window_limit_timeout(&self, value: i64) -> Result<&Self, AeronCError> {
        let key =
            std::ffi::CStr::from_bytes_until_nul(AERON_URI_UNTETHERED_WINDOW_LIMIT_TIMEOUT_KEY)
                .map_err(|_| AeronCError::from_code(PARSE_CSTR_ERROR_CODE))?;
        self.put_int64(key, value)?;
        Ok(self)
    }
    pub fn untethered_resting_timeout(&self, value: i64) -> Result<&Self, AeronCError> {
        let key = std::ffi::CStr::from_bytes_until_nul(AERON_URI_UNTETHERED_RESTING_TIMEOUT_KEY)
            .map_err(|_| AeronCError::from_code(PARSE_CSTR_ERROR_CODE))?;
        self.put_int64(key, value)?;
        Ok(self)
    }
    pub fn max_resend(&self, value: i32) -> Result<&Self, AeronCError> {
        let key = std::ffi::CStr::from_bytes_until_nul(AERON_URI_MAX_RESEND_KEY)
            .map_err(|_| AeronCError::from_code(PARSE_CSTR_ERROR_CODE))?;
        self.put_int32(key, value)?;
        Ok(self)
    }
    pub fn stream_id(&self, value: i32) -> Result<&Self, AeronCError> {
        let key = std::ffi::CStr::from_bytes_until_nul(AERON_URI_STREAM_ID_KEY)
            .map_err(|_| AeronCError::from_code(PARSE_CSTR_ERROR_CODE))?;
        self.put_int32(key, value)?;
        Ok(self)
    }
    pub fn publication_window(&self, value: i32) -> Result<&Self, AeronCError> {
        let key = std::ffi::CStr::from_bytes_until_nul(AERON_URI_PUBLICATION_WINDOW_KEY)
            .map_err(|_| AeronCError::from_code(PARSE_CSTR_ERROR_CODE))?;
        self.put_int32(key, value)?;
        Ok(self)
    }

    #[inline]
    pub fn build_into(&self, dst: &mut String) -> Result<(), AeronCError> {
        self.sprint_into(dst)?;
        Ok(())
    }
}

impl AeronCountersReader {
    #[inline]
    #[doc = "Get the label for a counter."]
    #[doc = ""]
    #[doc = " \n**param** counters_reader that contains the counter"]
    #[doc = " \n**param** counter_id to find"]
    #[doc = " \n**param** buffer to store the counter in."]
    #[doc = " \n**param** buffer_length length of the output buffer"]
    #[doc = " \n**return** -1 on failure, number of characters copied to buffer on success."]
    pub fn get_counter_label(
        &self,
        counter_id: i32,
        max_length: usize,
    ) -> Result<String, AeronCError> {
        let mut result = String::with_capacity(max_length);
        self.get_counter_label_into(counter_id, &mut result)?;
        Ok(result)
    }

    #[inline]
    #[doc = "Get the label for a counter."]
    pub fn get_counter_label_into(
        &self,
        counter_id: i32,
        dst: &mut String,
    ) -> Result<(), AeronCError> {
        unsafe {
            let capacity = dst.capacity();
            let vec = dst.as_mut_vec();
            vec.set_len(capacity);
            self.counter_label(counter_id, vec.as_mut_ptr() as *mut _, capacity)?;
            let mut len = 0;
            loop {
                if len == capacity {
                    break;
                }
                let val = vec[len];
                if val == 0 {
                    break;
                }
                len += 1;
            }
            vec.set_len(len);
        }
        Ok(())
    }

    #[inline]
    #[doc = "Get the key for a counter."]
    pub fn get_counter_key(&self, counter_id: i32) -> Result<Vec<u8>, AeronCError> {
        let mut dst = Vec::new();
        self.get_counter_key_into(counter_id, &mut dst)?;
        Ok(dst)
    }

    #[inline]
    #[doc = "Get the key for a counter."]
    pub fn get_counter_key_into(
        &self,
        counter_id: i32,
        dst: &mut Vec<u8>,
    ) -> Result<(), AeronCError> {
        let mut key_ptr: *mut u8 = std::ptr::null_mut();
        unsafe {
            let result = bindings::aeron_counters_reader_metadata_key(
                self.get_inner(),
                counter_id,
                &mut key_ptr,
            );
            if result < 0 || key_ptr.is_null() {
                return Err(AeronCError::from_code(result));
            }

            loop {
                let val = *key_ptr.add(dst.len());
                if val == 0 {
                    break;
                } else {
                    dst.push(val);
                }
            }
            Ok(())
        }
    }

    #[inline]
    pub fn get_counter_value(&self, counter_id: i32) -> i64 {
        unsafe { *self.addr(counter_id) }
    }
}

impl Aeron {
    pub fn new_blocking(
        context: &AeronContext,
        timeout: std::time::Duration,
    ) -> Result<Self, AeronCError> {
        if let Ok(aeron) = Aeron::new(&context) {
            return Ok(aeron);
        }
        let time = std::time::Instant::now();
        while time.elapsed() < timeout {
            if let Ok(aeron) = Aeron::new(&context) {
                return Ok(aeron);
            }
            #[cfg(debug_assertions)]
            std::thread::sleep(std::time::Duration::from_millis(10));
        }
        log::error!("failed to create aeron client for {:?}", context);
        Err(AeronErrorType::TimedOut.into())
    }
}

impl AeronFragmentHandlerCallback for AeronFragmentAssembler {
    fn handle_aeron_fragment_handler(&mut self, buffer: &[u8], header: AeronHeader) -> () {
        unsafe {
            aeron_fragment_assembler_handler(
                self.get_inner() as *mut _,
                buffer.as_ptr(),
                buffer.len(),
                header.get_inner(),
            )
        }
    }
}

impl AeronControlledFragmentHandlerCallback for AeronControlledFragmentAssembler {
    fn handle_aeron_controlled_fragment_handler(
        &mut self,
        buffer: &[u8],
        header: AeronHeader,
    ) -> aeron_controlled_fragment_handler_action_t {
        unsafe {
            aeron_controlled_fragment_assembler_handler(
                self.get_inner() as *mut _,
                buffer.as_ptr(),
                buffer.len(),
                header.get_inner(),
            )
        }
    }
}

impl<T: AeronFragmentHandlerCallback> Handler<T> {
    pub fn leak_with_fragment_assembler(
        handler: T,
    ) -> Result<(Handler<AeronFragmentAssembler>, Handler<T>), AeronCError> {
        let handler = Handler::leak(handler);
        Ok((
            Handler::leak(AeronFragmentAssembler::new(Some(&handler))?),
            handler,
        ))
    }
}

impl<T: AeronControlledFragmentHandlerCallback> Handler<T> {
    pub fn leak_with_controlled_fragment_assembler(
        handler: T,
    ) -> Result<(Handler<AeronControlledFragmentAssembler>, Handler<T>), AeronCError> {
        let handler = Handler::leak(handler);
        Ok((
            Handler::leak(AeronControlledFragmentAssembler::new(Some(&handler))?),
            handler,
        ))
    }
}

impl AeronBufferClaim {
    #[inline]
    pub fn data_mut(&self) -> &mut [u8] {
        debug_assert!(!self.data.is_null());
        unsafe { std::slice::from_raw_parts_mut(self.data, self.length) }
    }

    #[inline]
    pub fn frame_header_mut(&self) -> &mut aeron_header_values_frame_t {
        unsafe { &mut *self.frame_header.cast::<aeron_header_values_frame_t>() }
    }
}

pub struct AeronErrorLogger;
impl AeronErrorHandlerCallback for AeronErrorLogger {
    fn handle_aeron_error_handler(&mut self, error_code: std::ffi::c_int, msg: &str) -> () {
        log::error!("aeron error {}: {}", error_code, msg);
    }
}
unsafe impl Send for AeronErrorLogger {}
unsafe impl Sync for AeronErrorLogger {}

pub struct FnMutMessageHandler {
    func: fn(*mut (), &[u8], AeronHeader),
    ctx: *mut (),
}

impl AeronFragmentHandlerCallback for FnMutMessageHandler {
    fn handle_aeron_fragment_handler(&mut self, buffer: &[u8], header: AeronHeader) -> () {
        self.call(buffer, header);
    }
}

impl FnMutMessageHandler {
    pub fn new() -> Self {
        Self {
            func: Self::noop,
            ctx: std::ptr::null_mut(),
        }
    }

    #[inline]
    /// SAFETY: you must make sure ctx lives longer than when `call` method is invoked
    pub fn set<T>(&mut self, ctx: &mut T, func: fn(&mut T, &[u8], AeronHeader)) -> &mut Self {
        self.func = Self::wrap::<T>(func);
        self.ctx = ctx as *mut T as *mut ();
        self
    }

    #[inline(always)]
    pub fn call(&mut self, msg: &[u8], header: AeronHeader) {
        (self.func)(self.ctx, msg, header);
    }

    #[inline]
    fn wrap<T>(
        f: fn(&mut T, &[u8], AeronHeader)
    ) -> fn(*mut (), &[u8], AeronHeader) {
        // SAFETY: `fn(&mut T,…)` and `fn(*mut(),…)` have the same ABI/representation
        unsafe { std::mem::transmute(f) }
    }

    fn noop(_: *mut (), _: &[u8], _: AeronHeader) {
        // default no-op handler
    }
}

pub struct AeronFragmentClosureAssembler {
    assembler: AeronFragmentAssembler,
    handler: Handler<FnMutMessageHandler>,
    assembler_handler: Handler<AeronFragmentAssembler>,
}

impl AeronFragmentClosureAssembler {
    pub fn new() -> Result<Self, AeronCError> {
        let handler = Handler::leak(FnMutMessageHandler::new());
        Ok(Self {
            assembler: AeronFragmentAssembler::new(Some(&handler))?,
            handler,
            assembler_handler: Handler {
                raw_ptr: std::ptr::null_mut(),
                should_drop: false,
            },
        })
    }

    pub fn process<T>(&mut self, ctx: &mut T, func: fn(&mut T, &[u8], AeronHeader)) -> Option<&Handler<AeronFragmentAssembler>> {
        self.handler.set(ctx, func);
        self.assembler_handler.raw_ptr = &mut self.assembler as *mut _;
        Some(&self.assembler_handler)
    }
}
impl Drop for AeronFragmentClosureAssembler {
    fn drop(&mut self) {
        self.handler.release();
    }
}

pub struct FnMutControlledMessageHandler {
    func: fn(*mut (), &[u8], AeronHeader) -> aeron_controlled_fragment_handler_action_t,
    ctx: *mut (),
}

impl FnMutControlledMessageHandler {
    pub fn new() -> Self {
        Self { func: Self::noop, ctx: std::ptr::null_mut() }
    }

    #[inline]
    /// SAFETY: caller must ensure `ctx` outlives any invocation of the provided function.
    pub fn set<T>(
        &mut self,
        ctx: &mut T,
        func: fn(&mut T, &[u8], AeronHeader) -> aeron_controlled_fragment_handler_action_t,
    ) -> &mut Self {
        self.func = Self::wrap::<T>(func);
        self.ctx = ctx as *mut T as *mut ();
        self
    }

    #[inline(always)]
    pub fn call(&mut self, msg: &[u8], header: AeronHeader) -> aeron_controlled_fragment_handler_action_t {
        (self.func)(self.ctx, msg, header)
    }

    #[inline]
    fn wrap<T>(
        f: fn(&mut T, &[u8], AeronHeader) -> aeron_controlled_fragment_handler_action_t,
    ) -> fn(*mut (), &[u8], AeronHeader) -> aeron_controlled_fragment_handler_action_t {
        unsafe { std::mem::transmute(f) }
    }

    fn noop(_: *mut (), _: &[u8], _: AeronHeader) -> aeron_controlled_fragment_handler_action_t {
        bindings::aeron_controlled_fragment_handler_action_en::AERON_ACTION_CONTINUE
    }
}

impl AeronControlledFragmentHandlerCallback for FnMutControlledMessageHandler {
    fn handle_aeron_controlled_fragment_handler(
        &mut self,
        buffer: &[u8],
        header: AeronHeader,
    ) -> aeron_controlled_fragment_handler_action_t {
        self.call(buffer, header)
    }
}

pub struct AeronControlledFragmentClosureAssembler {
    assembler: AeronControlledFragmentAssembler,
    handler: Handler<FnMutControlledMessageHandler>,
    assembler_handler: Handler<AeronControlledFragmentAssembler>,
}

impl AeronControlledFragmentClosureAssembler {
    pub fn new() -> Result<Self, AeronCError> {
        let handler = Handler::leak(FnMutControlledMessageHandler::new());
        Ok(Self {
            assembler: AeronControlledFragmentAssembler::new(Some(&handler))?,
            handler,
            assembler_handler: Handler { raw_ptr: std::ptr::null_mut(), should_drop: false },
        })
    }

    pub fn process<T>(
        &mut self,
        ctx: &mut T,
        func: fn(&mut T, &[u8], AeronHeader) -> aeron_controlled_fragment_handler_action_t,
    ) -> Option<&Handler<AeronControlledFragmentAssembler>> {
        self.handler.set(ctx, func);
        self.assembler_handler.raw_ptr = &mut self.assembler as *mut _;
        Some(&self.assembler_handler)
    }
}

impl Drop for AeronControlledFragmentClosureAssembler {
    fn drop(&mut self) {
        self.handler.release();
    }
}

impl std::fmt::Display for AeronCError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "Aeron error {}: {:?} - [lastErrMsg={}]", self.code, self.kind(), Aeron::errmsg())
    }
}

impl std::fmt::Debug for AeronCError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("AeronCError")
            .field("code", &self.code)
            .field("kind", &self.kind())
            .field("last_error_msg", &Aeron::errmsg())
            .finish()
    }
}

impl std::error::Error for AeronCError {}