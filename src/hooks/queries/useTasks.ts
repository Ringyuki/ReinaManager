import {
	queryOptions,
	useMutation,
	useQuery,
	useQueryClient,
} from "@tanstack/react-query";
import { useCallback } from "react";
import {
	type Task,
	type TaskProgressEvent,
	type TaskStatus,
	taskService,
} from "@/services/invoke";

const activeTaskStatuses = new Set<TaskStatus>([
	"pending",
	"running",
	"paused",
]);
const resetDownloadErrorCodes = new Set([
	"checksum_mismatch",
	"size_mismatch",
	"url_expired",
]);
const ACTIVE_DOWNLOAD_PROGRESS_RATIO = 0.999;

type MergeTaskOptions = {
	beginRecovery?: boolean;
	resetDisplayedProgress?: boolean;
};

type UpdateTaskOptions = {
	beginRecovery?: boolean;
	resetDiscardedDownload?: boolean;
};

function isActiveDownloadTask(task: Task) {
	return (
		activeTaskStatuses.has(task.status) &&
		task.stage === "downloading" &&
		task.progress_unit === "bytes"
	);
}

function clampDisplayedProgress(
	task: Task,
	progress: number,
	activeDownload = isActiveDownloadTask(task),
) {
	const total = task.progress_total;
	if (total == null || total <= 0) return Math.max(0, progress);
	const maximum = activeDownload
		? Math.floor(total * ACTIVE_DOWNLOAD_PROGRESS_RATIO)
		: total;
	return Math.min(maximum, Math.max(0, progress));
}

function mergeTask(
	cached: Task | undefined,
	incoming: Task,
	{
		beginRecovery = false,
		resetDisplayedProgress = false,
	}: MergeTaskOptions = {},
): Task {
	const cachedActiveDownload = cached != null && isActiveDownloadTask(cached);
	const incomingActiveDownload = isActiveDownloadTask(incoming);
	const incomingActiveStatus = activeTaskStatuses.has(incoming.status);
	// 较早发起的轮询可能在实时下载事件之后才返回，此时空阶段仍按旧下载状态合并。
	const incomingMayStillBeDownloading =
		incoming.stage == null || incoming.stage === "downloading";
	const mergeCommittedProgress =
		!resetDisplayedProgress &&
		cached != null &&
		activeTaskStatuses.has(cached.status) &&
		incomingActiveStatus &&
		incomingMayStillBeDownloading &&
		(cachedActiveDownload || incomingActiveDownload);
	const progressCurrent = mergeCommittedProgress
		? Math.max(cached.progress_current, incoming.progress_current)
		: incoming.progress_current;
	const preserveCachedRealtime =
		cachedActiveDownload && incomingActiveStatus && incoming.stage == null;
	const keepRealtime = incomingActiveDownload || preserveCachedRealtime;
	const receivedBytes =
		!beginRecovery && keepRealtime
			? (incoming.received_bytes ?? cached?.received_bytes)
			: undefined;
	const cachedRunningDownload =
		cached?.status === "running" && cachedActiveDownload;
	const incomingRunningDownload =
		incoming.status === "running" && incomingActiveDownload;
	const bytesPerSecond = incomingRunningDownload
		? (incoming.bytes_per_second ??
			(cachedRunningDownload ? cached.bytes_per_second : undefined))
		: undefined;
	const merged = {
		...incoming,
		progress_current: progressCurrent,
		received_bytes: receivedBytes,
		bytes_per_second: bytesPerSecond,
	};
	const previousDisplayed = resetDisplayedProgress
		? 0
		: (cached?.displayed_progress ?? cached?.progress_current ?? 0);
	merged.displayed_progress = clampDisplayedProgress(
		merged,
		Math.max(previousDisplayed, progressCurrent, receivedBytes ?? 0),
		keepRealtime,
	);
	const recoveryTarget = beginRecovery
		? merged.displayed_progress
		: cached?.recovery_target;
	const caughtUpProgress = Math.max(progressCurrent, receivedBytes ?? 0);
	// 恢复期间固定展示水位，等新会话追上后再恢复常规进度与速度文案。
	merged.recovery_target =
		!resetDisplayedProgress &&
		keepRealtime &&
		recoveryTarget != null &&
		caughtUpProgress < recoveryTarget
			? recoveryTarget
			: undefined;
	return merged;
}

export const taskKeys = {
	all: ["tasks"] as const,
};

function tasksQueryOptions() {
	return queryOptions({
		queryKey: taskKeys.all,
		queryFn: () => taskService.listTasks(),
		structuralSharing: (previous, next) => {
			const previousTasks = previous as Task[] | undefined;
			const nextTasks = next as Task[];
			if (!previousTasks) {
				return nextTasks.map((task) => mergeTask(undefined, task));
			}
			const previousById = new Map(
				previousTasks.map((task) => [task.id, task]),
			);
			return nextTasks.map((task) =>
				mergeTask(previousById.get(task.id), task),
			);
		},
	});
}

type UseTasksOptions = {
	enabled?: boolean;
	pollActive?: boolean;
};

export function useTasks({
	enabled = true,
	pollActive = false,
}: UseTasksOptions = {}) {
	return useQuery({
		...tasksQueryOptions(),
		enabled,
		refetchInterval: pollActive
			? (query) =>
					query.state.data?.some((task) => activeTaskStatuses.has(task.status))
						? 1500
						: false
			: false,
	});
}

export function useActiveTaskCount() {
	const tasksQuery = useTasks();
	return {
		...tasksQuery,
		data: tasksQuery.data?.reduce(
			(count, task) => count + Number(activeTaskStatuses.has(task.status)),
			0,
		),
	};
}

export function useTaskCache() {
	const queryClient = useQueryClient();

	const fetchTasks = useCallback(
		() => queryClient.fetchQuery({ ...tasksQueryOptions(), staleTime: 0 }),
		[queryClient],
	);
	const prependTask = useCallback(
		(task: Task) => {
			queryClient.setQueryData<Task[]>(taskKeys.all, (current) => {
				if (!current) return [mergeTask(undefined, task)];
				return current.some((item) => item.id === task.id)
					? current
					: [mergeTask(undefined, task), ...current];
			});
		},
		[queryClient],
	);
	const updateTask = useCallback(
		(
			updated: Task,
			{
				beginRecovery = false,
				resetDiscardedDownload = false,
			}: UpdateTaskOptions = {},
		) => {
			queryClient.setQueryData<Task[]>(taskKeys.all, (current) =>
				current?.map((task) => {
					if (task.id !== updated.id) return task;
					const resetDisplayedProgress =
						resetDiscardedDownload &&
						resetDownloadErrorCodes.has(task.error_code ?? "");
					return mergeTask(task, updated, {
						beginRecovery,
						resetDisplayedProgress,
					});
				}),
			);
		},
		[queryClient],
	);
	const updateTaskProgress = useCallback(
		(event: TaskProgressEvent) => {
			queryClient.setQueryData<Task[]>(taskKeys.all, (current) =>
				current?.map((task) => {
					if (task.id !== event.task_id) return task;
					return mergeTask(task, {
						...task,
						status: event.status,
						stage: event.stage,
						progress_current: event.progress_current,
						progress_total: event.progress_total,
						progress_unit: event.progress_unit,
						bytes_per_second: event.bytes_per_second,
						received_bytes: event.received_bytes,
					});
				}),
			);
		},
		[queryClient],
	);
	const removeTask = useCallback(
		(taskId: number) => {
			queryClient.setQueryData<Task[]>(taskKeys.all, (current) =>
				current?.filter((task) => task.id !== taskId),
			);
		},
		[queryClient],
	);
	const invalidateTasks = useCallback(
		() => queryClient.invalidateQueries({ queryKey: taskKeys.all }),
		[queryClient],
	);

	return {
		fetchTasks,
		prependTask,
		updateTask,
		updateTaskProgress,
		removeTask,
		invalidateTasks,
	};
}

export type TaskAction = "pause" | "resume" | "cancel" | "retry" | "delete";

type TaskActionVariables = {
	taskId: number;
	action: TaskAction;
	archivePassword?: string;
};

async function executeTaskAction({
	taskId,
	action,
	archivePassword,
}: TaskActionVariables): Promise<Task | undefined> {
	switch (action) {
		case "pause":
			return await taskService.pauseTask(taskId);
		case "resume":
			return await taskService.resumeTask(taskId);
		case "cancel":
			return await taskService.cancelTask(taskId);
		case "retry":
			return await taskService.retryTask(taskId, undefined, archivePassword);
		case "delete":
			await taskService.deleteTask(taskId);
			return undefined;
	}
}

export function useTaskActions() {
	const { removeTask, updateTask } = useTaskCache();

	return useMutation({
		mutationFn: executeTaskAction,
		onSuccess: (updated, { taskId, action }) => {
			if (action === "delete") {
				removeTask(taskId);
			} else if (updated) {
				updateTask(updated, {
					beginRecovery: action === "resume",
					resetDiscardedDownload: action === "retry",
				});
			}
		},
	});
}
