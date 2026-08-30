import SwapHorizIcon from "@mui/icons-material/SwapHoriz";
import {
	Box,
	Button,
	Chip,
	Dialog,
	DialogActions,
	DialogContent,
	DialogTitle,
	Divider,
	Popover,
	Stack,
	Typography,
	useMediaQuery,
	useTheme,
} from "@mui/material";
import { useId, useState } from "react";
import { useTranslation } from "react-i18next";
import { getRuntimeSourceAdapter } from "@/metadata";
import type { SourceSummaryOption } from "@/metadata/data/displayMergeRules";
import type { SourceType } from "@/types";

const CARRIAGE_RETURN_PATTERN = /\r\n?/g;

function countSummaryCharacters(summary: string): number {
	const normalizedSummary = summary.replace(CARRIAGE_RETURN_PATTERN, "\n");
	return Array.from(normalizedSummary).length;
}

interface SourceSummarySelectorProps {
	options: SourceSummaryOption[];
	currentSummary: string;
	disabled: boolean;
	onSelect: (summary: string) => void;
}

interface SourceSummarySelectorContentProps {
	options: SourceSummaryOption[];
	selectedSource: SourceType;
	titleId: string;
	onSourceChange: (source: SourceType) => void;
	onCancel: () => void;
	onConfirm: () => void;
}

function getSourceLabel(source: SourceType): string {
	return getRuntimeSourceAdapter(source).label;
}

function SourceSummarySelectorContent({
	options,
	selectedSource,
	titleId,
	onSourceChange,
	onCancel,
	onConfirm,
}: SourceSummarySelectorContentProps) {
	const { t } = useTranslation();
	const selectedOption =
		options.find((option) => option.source === selectedSource) ?? options[0];

	if (!selectedOption) return null;

	return (
		<>
			<DialogTitle id={titleId} className="pb-2">
				{t(
					"pages.Detail.GameInfoEdit.selectSourceSummaryTitle",
					"选择游戏简介",
				)}
			</DialogTitle>
			<DialogContent>
				<Stack spacing={2}>
					<Box className="flex flex-wrap gap-2">
						{options.map((option) => {
							const selected = option.source === selectedOption.source;
							return (
								<Chip
									key={option.source}
									label={getSourceLabel(option.source)}
									clickable
									color={selected ? "primary" : "default"}
									variant={selected ? "filled" : "outlined"}
									onClick={() => onSourceChange(option.source)}
								/>
							);
						})}
					</Box>
					<Box>
						<Typography variant="subtitle2">
							{getSourceLabel(selectedOption.source)} ·{" "}
							{t(
								"pages.Detail.GameInfoEdit.summaryCharacterCount",
								"{{length}} 字",
								{
									length: countSummaryCharacters(selectedOption.summary),
								},
							)}
						</Typography>
					</Box>
					<Divider />
					<Typography
						variant="body2"
						className="max-h-72 overflow-y-auto whitespace-pre-wrap break-words pr-1"
					>
						{selectedOption.summary}
					</Typography>
				</Stack>
			</DialogContent>
			<DialogActions>
				<Button onClick={onCancel}>
					{t("pages.Detail.GameInfoEdit.cancel", "取消")}
				</Button>
				<Button variant="contained" onClick={onConfirm}>
					{t("pages.Detail.GameInfoEdit.useSourceSummary", "使用此简介")}
				</Button>
			</DialogActions>
		</>
	);
}

export function SourceSummarySelector({
	options,
	currentSummary,
	disabled,
	onSelect,
}: SourceSummarySelectorProps) {
	const { t } = useTranslation();
	const theme = useTheme();
	const isNarrowScreen = useMediaQuery(theme.breakpoints.down("sm"));
	const titleId = useId();
	const [anchorEl, setAnchorEl] = useState<HTMLElement | null>(null);
	const [selectedSource, setSelectedSource] = useState<SourceType | null>(null);
	const open = Boolean(anchorEl);

	if (options.length < 2) return null;

	const handleOpen = (event: React.MouseEvent<HTMLButtonElement>) => {
		const matchingOption = options.find(
			(option) => option.summary === currentSummary,
		);
		setSelectedSource(matchingOption?.source ?? options[0].source);
		setAnchorEl(event.currentTarget);
	};

	const handleClose = () => {
		setAnchorEl(null);
	};

	const handleConfirm = () => {
		const selectedOption =
			options.find((option) => option.source === selectedSource) ?? options[0];
		if (!selectedOption) return;

		onSelect(selectedOption.summary);
		handleClose();
	};

	const resolvedSelectedSource = options.some(
		(option) => option.source === selectedSource,
	)
		? selectedSource
		: options[0].source;
	const content = resolvedSelectedSource ? (
		<SourceSummarySelectorContent
			options={options}
			selectedSource={resolvedSelectedSource}
			titleId={titleId}
			onSourceChange={setSelectedSource}
			onCancel={handleClose}
			onConfirm={handleConfirm}
		/>
	) : null;

	return (
		<>
			<Button
				variant="outlined"
				size="small"
				startIcon={<SwapHorizIcon />}
				onClick={handleOpen}
				disabled={disabled}
				aria-haspopup="dialog"
				aria-expanded={open}
			>
				{t("pages.Detail.GameInfoEdit.selectSourceSummary", "从数据源选择")}
			</Button>

			{isNarrowScreen ? (
				<Dialog
					open={open}
					onClose={handleClose}
					fullWidth
					maxWidth="sm"
					aria-labelledby={titleId}
				>
					{content}
				</Dialog>
			) : (
				<Popover
					open={open}
					anchorEl={anchorEl}
					onClose={handleClose}
					anchorOrigin={{ vertical: "bottom", horizontal: "right" }}
					transformOrigin={{ vertical: "top", horizontal: "right" }}
					slotProps={{
						paper: {
							className: "mt-1 w-140 max-w-[calc(100vw-32px)]",
							role: "dialog",
							"aria-labelledby": titleId,
						},
					}}
				>
					{content}
				</Popover>
			)}
		</>
	);
}
