mod common;

use common::{TempDir, git, init_repo_with_commit};
use herdr_file_viewer::git::{Baseline, Status, changed_set_scoped, diff_directory, status_scoped};
use std::fs;
use std::path::{Path, PathBuf};

#[test]
fn nested_scope_keeps_git_markers_and_baselines_without_enumerating_siblings() {
    let repo = TempDir::new();
    init_repo_with_commit(repo.path());
    git(repo.path(), &["branch", "-M", "main"]);
    let scope = Path::new("area [x]/umts space");
    let sibling = Path::new("area x/umts space");
    for dir in [scope, sibling] {
        fs::create_dir_all(repo.path().join(dir)).unwrap();
        for file in ["branch.txt", "edit.txt", "delete.txt"] {
            fs::write(repo.path().join(dir).join(file), "original\n").unwrap();
        }
    }
    git(repo.path(), &["add", "."]);
    git(repo.path(), &["commit", "-qm", "base files"]);
    git(repo.path(), &["checkout", "-qb", "feature"]);
    for dir in [scope, sibling] {
        fs::write(
            repo.path().join(dir).join("branch.txt"),
            "committed change\n",
        )
        .unwrap();
    }
    git(repo.path(), &["add", "."]);
    git(repo.path(), &["commit", "-qm", "branch changes"]);
    for dir in [scope, sibling] {
        fs::write(repo.path().join(dir).join("edit.txt"), "edited\n").unwrap();
        fs::remove_file(repo.path().join(dir).join("delete.txt")).unwrap();
        fs::write(repo.path().join(dir).join("new.txt"), "untracked\n").unwrap();
    }

    let status = status_scoped(repo.path(), scope);
    assert_eq!(status.len(), 3);
    assert_eq!(status.get(&scope.join("edit.txt")), Some(&Status::Modified));
    assert_eq!(
        status.get(&scope.join("delete.txt")),
        Some(&Status::Deleted)
    );
    assert_eq!(status.get(&scope.join("new.txt")), Some(&Status::Untracked));
    assert_eq!(
        changed_set_scoped(repo.path(), Baseline::Head, Some("main"), scope),
        status
    );
    let base = changed_set_scoped(repo.path(), Baseline::Base, Some("main"), scope);
    assert_eq!(base.len(), 4);
    assert_eq!(base.get(&scope.join("branch.txt")), Some(&Status::Modified));
    assert!(base.keys().all(|path| path.starts_with(scope)));

    let diff = diff_directory(repo.path(), scope, Baseline::Head, None);
    assert!(diff.contains("edited"));
    assert!(diff.contains("untracked"));
    assert!(!diff.contains("area x/"));
}

#[test]
fn scoped_enumeration_rejects_paths_outside_the_worktree() {
    let repo = TempDir::new();
    init_repo_with_commit(repo.path());
    fs::write(repo.path().join("seed.txt"), "changed\n").unwrap();
    for scope in [PathBuf::from("../"), repo.path().to_path_buf()] {
        assert!(status_scoped(repo.path(), &scope).is_empty());
        assert!(changed_set_scoped(repo.path(), Baseline::Head, None, &scope).is_empty());
    }
}
