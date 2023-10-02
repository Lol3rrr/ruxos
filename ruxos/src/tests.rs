pub mod mpsc {
    use rand::{Rng, SeedableRng};

    pub struct FallibleSender<T> {
        tx: std::sync::mpsc::Sender<T>,
    }

    pub struct FallibleReceiver<T> {
        rx: std::sync::mpsc::Receiver<T>,
        ratio: f64,
        rand: rand::rngs::SmallRng,
    }

    impl<T> Clone for FallibleSender<T> {
        fn clone(&self) -> Self {
            Self {
                tx: self.tx.clone(),
            }
        }
    }

    pub fn queue<T>(ratio: f64, seed: u64) -> (FallibleSender<T>, FallibleReceiver<T>) {
        let (tx, rx) = std::sync::mpsc::channel();

        assert!(
            ratio <= 1.0,
            "The given Ratio needs to be less than or equal to 1.0 / 100% but was {}",
            ratio
        );
        assert!(
            ratio >= 0.0,
            "The given Ratio needs to be greater than or equal to 0.0 / 0% but was {}",
            ratio
        );

        (
            FallibleSender { tx },
            FallibleReceiver {
                rx,
                ratio,
                rand: rand::rngs::SmallRng::seed_from_u64(seed),
            },
        )
    }

    impl<T> FallibleSender<T> {
        pub fn send(&self, val: T) -> Result<(), std::sync::mpsc::SendError<T>> {
            self.tx.send(val)
        }
    }

    impl<T> FallibleReceiver<T> {
        pub fn try_recv(&mut self) -> Result<T, std::sync::mpsc::TryRecvError> {
            let val = self.rx.try_recv()?;

            let rand_val: f64 = self.rand.gen_range(0.0..1.0);

            if rand_val <= self.ratio {
                Ok(val)
            } else {
                Err(std::sync::mpsc::TryRecvError::Empty)
            }
        }

        pub fn recv(&mut self) -> Result<T, std::sync::mpsc::RecvError> {
            loop {
                let val = self.rx.recv()?;

                let rand_val: f64 = self.rand.gen_range(0.0..1.0);

                if rand_val <= self.ratio {
                    return Ok(val);
                }
            }
        }
    }

    #[cfg(test)]
    mod testing {
        use super::*;

        #[test]
        fn infallible() {
            let (tx, mut rx) = queue(1.0, 0);

            for i in 0..1000 {
                tx.send(i).unwrap();
            }

            for i in 0..1000 {
                assert_eq!(Ok(i), rx.recv());
            }
        }

        #[test]
        fn fallible() {
            let (tx, mut rx) = queue(0.0, 0);

            for i in 0..1000 {
                tx.send(i).unwrap();
            }

            for _ in 0..1000 {
                rx.try_recv().expect_err("");
            }
        }

        #[test]
        fn ratio() {
            let (tx, mut rx) = queue(0.5, 0);

            for i in 0..1000 {
                tx.send(i).unwrap();
            }

            let failures = (0..1000).filter(|_| rx.try_recv().is_err()).count();

            assert!(
                failures > 450 && failures < 550,
                "Expected around 450-550 failures but got {}",
                failures
            );
        }
    }
}
