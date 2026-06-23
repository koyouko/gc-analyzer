import os


ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))


def test_html_escape_helper_escapes_markup_characters():
    with open(os.path.join(ROOT, "frontend", "index.html")) as fh:
        html = fh.read()

    assert "replace(/&/g" in html
    assert "replace(/</g" in html
    assert "replace(/>/g" in html
    assert "replace(/\"/g" in html
    assert "replace(/'/g" in html


def test_inline_handlers_use_escaped_javascript_arguments():
    with open(os.path.join(ROOT, "frontend", "index.html")) as fh:
        html = fh.read()

    assert "selectCluster('" not in html
    assert "selectInstance('" not in html
    assert "openCancelJobModal('" not in html
    assert "openJobLogsModal('" not in html
    assert "editClusterConfig('" not in html
    assert "openDeleteConfirmModal('" not in html
    assert 'onclick="toggle(\'' not in html
    assert "selectLogTab('" not in html
    assert "setExplorerTab('" not in html


def test_instance_detail_escapes_strings_and_encodes_urls():
    with open(os.path.join(ROOT, "frontend", "index.html")) as fh:
        html = fh.read()

    assert "api('/api/instance/'+id)" not in html
    assert "api('/api/instance/'+CURRENT_INSTANCE" not in html
    assert "Loading '+id" not in html
    assert "${inst.id}" not in html
    assert "${a.msg}" not in html
    assert "${h.reasons.join" not in html
    assert "encodeURIComponent(id)" in html
    assert "encodeURIComponent(CURRENT_INSTANCE)" in html
