from __future__ import annotations

from collections.abc import Iterable, Iterator
from contextlib import contextmanager

from rich.live import Live
from rich.progress import (
    BarColumn,
    MofNCompleteColumn,
    Progress,
    ProgressColumn,
    TaskID,
    TextColumn,
    TimeElapsedColumn,
)
from rich.tree import Tree

from .import_datasets import DatasetImportEvent
from .import_dimension_records import DimensionRecordImportProgress


class DataReleaseImportProgressDisplay:
    def __init__(self, dataset_type_count: int) -> None:
        self._top_level = Tree("Import data release")

        self._dataset_type_progress = IndeterminateProgressDisplay("Dataset type import")
        self._top_level.add(self._dataset_type_progress.get_renderable())

        self._dimension_progress = NestedProgressDisplay(
            "Dimension record import", self._progress_columns(), self._progress_columns()
        )
        self._top_level.add(self._dimension_progress.get_renderable())

        self._collection_progress = IndeterminateProgressDisplay("Collection import")
        self._top_level.add(self._collection_progress.get_renderable())

        self._dataset_progress = NestedProgressDisplay(
            "Dataset import", self._progress_columns(), self._progress_columns()
        )
        self._dataset_progress.update_parent(total=dataset_type_count, completed=0)
        self._top_level.add(self._dataset_progress.get_renderable())

    def _progress_columns(self) -> list[ProgressColumn]:
        return [
            TextColumn("{task.description}"),
            BarColumn(),
            MofNCompleteColumn(),
            TimeElapsedColumn(),
        ]

    @contextmanager
    def run(self) -> Iterator[DataReleaseImportProgressDisplay]:
        with Live(self._top_level):
            yield self

    def update_dimension_record_progress(self, progress: DimensionRecordImportProgress) -> None:
        self._dimension_progress.update_parent(progress.total_dimensions, progress.completed_dimensions)
        for dimension, child_progress in progress.dimension_progress.items():
            self._dimension_progress.update_child(
                dimension,
                total=child_progress.total_rows,
                completed=child_progress.completed_rows,
                visible=child_progress.status != "complete",
            )

    def mark_collection_import_complete(self) -> None:
        self._collection_progress.mark_complete()

    def mark_dataset_type_complete(self) -> None:
        self._dataset_type_progress.mark_complete()

    def handle_dataset_import_event(self, event: DatasetImportEvent) -> None:
        match event.event:
            case "started":
                self._dataset_progress.update_child(
                    event.dataset_type, total=event.total_datasets, completed=0
                )
            case "progress":
                self._dataset_progress.advance_child(event.dataset_type, event.datasets_imported)
            case "completed":
                self._dataset_progress.remove_child(event.dataset_type)
                self._dataset_progress.advance_parent(1)


class NestedProgressDisplay:
    """Displays a top-level progress bar representing a group of tasks, with
    multiple progress bars nested under it representing the individual
    tasks.
    """

    def __init__(
        self,
        parent_label: str,
        parent_columns: Iterable[ProgressColumn],
        child_columns: Iterable[ProgressColumn],
    ) -> None:
        self._parent = Progress(*parent_columns)
        self._parent_task = self._parent.add_task(parent_label, total=None)
        self._tree = Tree(self._parent)
        self._child_progress = Progress(*child_columns)
        self._tree.add(self._child_progress)
        self._child_tasks: dict[str, TaskID] = {}

    def get_renderable(self) -> Tree:
        return self._tree

    def update_parent(self, total: int | None, completed: int, visible: bool = True) -> None:
        self._parent.update(self._parent_task, total=total, completed=completed, visible=visible)

    def advance_parent(self, count: int) -> None:
        self._parent.update(self._parent_task, advance=count)

    def update_child(self, label: str, total: int | None, completed: int, visible: bool = True) -> None:
        task = self._child_tasks.get(label, None)
        if task is None:
            task = self._child_progress.add_task(label, visible=False)
            self._child_tasks[label] = task
        self._child_progress.update(task, total=total, completed=completed, visible=visible)

    def advance_child(self, label: str, count: int) -> None:
        """Increase the 'completed' count for the given child."""
        task = self._child_tasks[label]
        self._child_progress.update(task, advance=count)

    def remove_child(self, label: str) -> None:
        task = self._child_tasks.pop(label)
        if task is not None:
            self._child_progress.remove_task(task)


class IndeterminateProgressDisplay:
    """Renders a progress bar for a task that is running, but doesn't have a
    known percentage completion.
    """

    def __init__(self, title: str) -> None:
        self._progress = Progress(TextColumn(title), BarColumn(), TimeElapsedColumn())
        self._task = self._progress.add_task(title, total=None)

    def get_renderable(self) -> Progress:
        return self._progress

    def mark_complete(self) -> None:
        self._progress.update(self._task, total=1, completed=1)
