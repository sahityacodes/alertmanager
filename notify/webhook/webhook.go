// Notify implements the Notifier interface.
func (n *Notifier) Notify(ctx context.Context, alerts ...*types.Alert) (bool, error) {
    // Add debug log for incoming alerts
    n.logger.Debug("processing webhook notification", 
        "alert_count", len(alerts),
        "receiver", n.conf.Name,
    )

    alerts, numTruncated := truncateAlerts(n.conf.MaxAlerts, alerts)
    data := notify.GetTemplateData(ctx, n.tmpl, alerts, n.logger)

    groupKey, err := notify.ExtractGroupKey(ctx)
    if err != nil {
        n.logger.Error("failed to extract group key", "err", err)
    } else {
        n.logger.Debug("extracted group key", "group_key", groupKey.String())
    }

    msg := &Message{
        Version:         "4",
        Data:            data,
        GroupKey:        groupKey.String(),
        TruncatedAlerts: numTruncated,
    }

    var buf bytes.Buffer
    if err := json.NewEncoder(&buf).Encode(msg); err != nil {
        n.logger.Error("failed to encode message", "err", err)
        return false, err
    }

    // Log the complete payload being sent
    n.logger.Debug("webhook request payload",
        "payload", buf.String(),
        "max_alerts", n.conf.MaxAlerts,
        "truncated_alerts", numTruncated,
    )

    var url string
    if n.conf.URL != nil {
        url = n.conf.URL.String()
    } else {
        content, err := os.ReadFile(n.conf.URLFile)
        if err != nil {
            n.logger.Error("failed to read URL file", 
                "path", n.conf.URLFile,
                "err", err,
            )
            return false, fmt.Errorf("read url_file: %w", err)
        }
        url = strings.TrimSpace(string(content))
    }

    n.logger.Debug("preparing webhook request",
        "url", url,
        "timeout", n.conf.Timeout,
    )

    if n.conf.Timeout > 0 {
        postCtx, cancel := context.WithTimeoutCause(ctx, n.conf.Timeout, fmt.Errorf("configured webhook timeout reached (%s)", n.conf.Timeout))
        defer cancel()
        ctx = postCtx
    }

    // Log right before sending
    n.logger.Debug("sending webhook request",
        "url", notify.RedactURL(url),
        "content_length", buf.Len(),
    )

    resp, err := notify.PostJSON(ctx, n.client, url, &buf)
    if err != nil {
        n.logger.Error("webhook request failed",
            "url", notify.RedactURL(url),
            "err", err,
            "context_error", ctx.Err(),
            "context_cause", context.Cause(ctx),
        )
        if ctx.Err() != nil {
            err = fmt.Errorf("%w: %w", err, context.Cause(ctx))
        }
        return true, notify.RedactURL(err)
    }
    defer notify.Drain(resp)

    // Log response details
    bodyBytes, _ := io.ReadAll(resp.Body)
    n.logger.Debug("webhook response",
        "status_code", resp.StatusCode,
        "status", resp.Status,
        "response_body", string(bodyBytes),
        "url", notify.RedactURL(url),
    )

    shouldRetry, err := n.retrier.Check(resp.StatusCode, resp.Body)
    if err != nil {
        n.logger.Error("webhook notification failed",
            "should_retry", shouldRetry,
            "err", err,
            "status_code", resp.StatusCode,
            "failure_reason", notify.GetFailureReasonFromStatusCode(resp.StatusCode),
        )
        return shouldRetry, notify.NewErrorWithReason(notify.GetFailureReasonFromStatusCode(resp.StatusCode), err)
    }

    n.logger.Debug("webhook notification successful",
        "status_code", resp.StatusCode,
    )
    return shouldRetry, err
}