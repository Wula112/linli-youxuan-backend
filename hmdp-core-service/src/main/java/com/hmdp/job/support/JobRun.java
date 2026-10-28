package com.hmdp.job.support;

import com.xxl.job.core.context.XxlJobHelper;
import lombok.Getter;

import java.time.Duration;

/** 单次批处理的计数与软预算，不保存业务状态，也不负责重试。 */
@Getter
public class JobRun {
    private final long started = System.nanoTime();
    private final long budgetNanos;
    private int checked;
    private int skipped;
    private int failures;
    private boolean limited;
    private boolean interrupted;

    public JobRun(Duration budget) { this.budgetNanos = budget.toNanos(); }
    public void checked() { checked++; }
    public void skipped() { skipped++; }
    public void failed() { failures++; }
    public void limited() { limited = true; }

    public boolean canContinue() {
        if (Thread.currentThread().isInterrupted()) {
            interrupted = true;
            return false;
        }
        if (System.nanoTime() - started >= budgetNanos) {
            limited = true;
            return false;
        }
        return true;
    }

    public boolean successful() { return failures == 0 && !interrupted; }

    public void reportToXxl() {
        if (Thread.currentThread().isInterrupted()) interrupted = true;
        XxlJobHelper.log(toString());
        if (successful()) XxlJobHelper.handleSuccess(toString());
        else XxlJobHelper.handleFail(toString());
    }

    @Override
    public String toString() {
        return "检查=" + checked + "，跳过=" + skipped + "，失败=" + failures
                + "，达到批次时间预算=" + limited + "，被中断=" + interrupted;
    }
}
