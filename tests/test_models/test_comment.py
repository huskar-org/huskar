from __future__ import absolute_import

from huskar_api.models.comment import set_comment, get_comment, Comment


def test_create_comment(db):
    assert db.query(Comment).count() == 0
    set_comment('base.foo', 'alta-stable', 'config', 'DB_URL', u'\u86e4')

    assert db.query(Comment).count() == 1
    comment = db.query(Comment).first()
    assert comment.application == 'base.foo'
    assert comment.cluster == 'alta-stable'
    assert comment.key_type == 'config'
    assert comment.key_name == 'DB_URL'
    assert comment.key_comment == u'\u86e4'


def test_override_comment(db):
    assert db.query(Comment).count() == 0
    set_comment('base.foo', 'alta-stable', 'config', 'DB_URL', u'\u86e4')
    set_comment('base.foo', 'alta-stable', 'config', 'DB_URL', u'+1s')

    assert db.query(Comment).count() == 1
    comment = db.query(Comment).first()
    assert comment.application == 'base.foo'
    assert comment.cluster == 'alta-stable'
    assert comment.key_type == 'config'
    assert comment.key_name == 'DB_URL'
    assert comment.key_comment == u'+1s'


def test_delete_comment(db):
    assert db.query(Comment).count() == 0

    set_comment('base.foo', 'alta-stable', 'config', 'DB_URL', u'\u86e4')
    assert db.query(Comment).count() == 1

    set_comment('base.foo', 'alta-stable', 'config', 'DB_URL', None)
    assert db.query(Comment).count() == 0

    set_comment('base.foo', 'alta-stable', 'switch', 'DB_URL', None)
    assert db.query(Comment).count() == 0


def test_get_comment(db):
    stmt = Comment.__table__.insert().values(
        application='base.foo',
        cluster='test',
        key_type='switch',
        key_name='k',
        key_comment=u'\u957f\u8005'
    )
    db.execute(stmt)
    db.commit()

    assert get_comment('base.foo', 'test', 'config', 'k') == u''
    assert get_comment('base.foo', 'test', 'switch', 'k') == u'\u957f\u8005'
    assert get_comment('base.foo', 'test', 'switch', 'K') == u''
