package com.research.training;

import java.util.Spliterator;

public class SpliteratorThread<T> extends Thread {

    private Spliterator<T> mSpliterator;

    public SpliteratorThread(Spliterator<T> spliterator) {
        mSpliterator = spliterator;
    }


    @Override
    public void run() {
        super.run();
        if (mSpliterator != null) {
            mSpliterator.forEachRemaining(
                    t -> System.out.println(Thread.currentThread().getName() + "-" + t + " "));

        }

    }
}